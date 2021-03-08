#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
"""Implements gzip and Zstandard indexing, repacking and searching.

Background
----------

Ordinary gzip and Zstandard files are not searchable.  You have to read the file from the
beginning until you find the required record.

``gzipi`` works on compressed CSV and JSON files, expecting the files to contain a
single *record* per line.  Each record consists of multiple columns, in the
case of CSV, or fields, in the case of JSON.  ``gzipi`` picks one of these
columns/fields as the **index key**.

Using ``gzipi``
---------------

Before you can use ``gzipi`` to search your compressed CSV or JSON files, you must
**repack** them.  Repacking break ordinary gzip files into multiple chunks,
where each chunk is like an ordinary gzip file.  While doing this, ``gzipi``
builds an index, keeping track of chunks and keys.  More specifically, for each
key, the index will contain:

 - ``gzip_start_offset``: the start of chunk that contains the key
 - ``gzip_length``: the length of the chunk
 - ``line_start_offset``: the start of the CSV/JSON record, relative to the
   beginning of the chunk
 - ``line_length``: the length of the record.

All offsets and lengths are in bytes.
The index is therefore CSV in the following format::

    key|gzip_start_offset|gzip_length|line_start_offset|line_length

Finally, ``gzipi`` concatenates all the chunks to create the repacked file.
This repacked file is fully compatible with the ``gzip`` family of tools, and
behaves like the original compressed CSV or JSON file.

The main entry points of this module are:

- index_csv_file, index_json_file: Scan a file and create a new index file.
- repack_json_file, repack_csv_file: Recompress a compressed file and create a new
  index for it.
- search: Look up a single key in the index.
- retrieve: Use a previously created index to quickly access individual lines
  in the compressed file.

"""

import collections
import csv
import distutils.spawn
import enum
import functools
import gzip
import io
import json
import logging
import multiprocessing
import os
import shutil
import struct
import sys
import tempfile
import time

from typing import (
    Any,
    Callable,
    cast,
    Dict,
    IO,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import botocore.exceptions
import plumbum
import smart_open
import zstandard

_GZIP_HEADER = b'\x1f\x8b\x08'
"""A magic gzip header and two compression flags.

Offset   Length   Contents
  0      2 bytes  magic header  0x1f, 0x8b (\037 \213)
  2      1 byte   compression method
                     0: store (copied)
                     1: compress
                     2: pack
                     3: lzh
                     4..7: reserved
                     8: deflate
  3      1 byte   flags
                     bit 0 set: file probably ascii text
                     bit 1 set: continuation of multi-part gzip file, part number present
                     bit 2 set: extra field present
                     bit 3 set: original file name present
                     bit 4 set: file comment present
                     bit 5 set: file is encrypted, encryption header present
                     bit 6,7:   reserved
"""
_ZSTD_HEADER = b'(\xb5/\xfd'

_GZIP_HEADER_LENGTH = 10
"""The length of main fields of gzip header, in bytes."""
_ZSTD_HEADER_LENGTH = 18
"""The maximum header length of the Zstandard header.

https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#frame_header
"""
_WINDOW_OFFSET_GZIP = (_GZIP_HEADER_LENGTH - 1) * -1
_WINDOW_OFFSET_ZST = (_ZSTD_HEADER_LENGTH - 1) * -1
_MIN_CHUNK_SIZE = 100000
"""The minimum amount of bytes in each chunk.

If this value is larger than the actual minimum size, it's possible that two chunks
will be joined into one.
"""

FILE_FORMATS = ('csv', 'json')
"""Supported file formats."""

DEFAULT_CSV_COLUMN = 0
"""The number of the column to use for indexing CSV."""

DEFAULT_CSV_DELIMITER = '|'
"""The character used for delimiting CSV columns."""

DEFAULT_JSON_FIELD = 'domain'
"""The field to use when indexing JSON."""

_OLDEST_UNIX_TIMESTAMP = 1262307600
"""All Unix timestamps in the gzip header that are smaller than this value are treated as malformed.

Currently, set to 2010-01-01.
"""

_POSSIBLE_OS_TYPES = (0x00, 0x03, 0x07, 0xFF)
"""Possible values for Operating System field in the gzip header.

Currently, set to Windows, Unix, Macintosh and Other.
"""
_TEXT_ENCODING = "utf-8"

DEFAULT_CHUNK_SIZE = 5000
"""The number of lines to pack in a single gzip chunk."""

_MAX_RECORDS_PER_BATCH = 5000
"""The maximum number of records to retrieve in a single batch."""

_SORT_CPU_COUNT = multiprocessing.cpu_count()
_SORT_BUFFER_SIZE = '1G'

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())

csv.field_size_limit(sys.maxsize)

_DEFAULT_BUFFER_SIZE = 1024
"""The maximum size of the index file to load in memory when performing binary search, in KiB."""

_BYTES_IN_KiB = 1024

_LINE_TERMINATOR = b'\n'


class Compression(enum.Enum):
    NONE = None
    GZIP = 'gzip'
    ZSTD = 'zstd'


SUPPORTED_COMPRESSIONS = {Compression.GZIP.value, Compression.ZSTD.value, }


def assume_compression_from_filename(path: str) -> Optional[str]:
    """Get compression format based on the extension."""
    if not isinstance(path, str):
        return Compression.NONE.value
    if path.endswith('.gz'):
        return Compression.GZIP.value
    elif path.endswith('.zst'):
        return Compression.ZSTD.value
    else:
        return Compression.NONE.value


def _get_compression_type(path: str) -> Optional[str]:
    with open(path, 'rb') as fin:
        return _determine_compression_from_header(fin)


def _determine_compression_from_header(fin: IO[bytes]) -> Optional[str]:
    header = fin.read(4)
    fin.seek(0)
    if header.startswith(_GZIP_HEADER):
        return Compression.GZIP.value
    elif header.startswith(_ZSTD_HEADER):
        return Compression.ZSTD.value
    return Compression.NONE.value


def _open_compressed_file(
    path: Union[str, IO],
    compression: Optional[str],
    mode: str = 'r'
) -> IO:
    """Open the specified file for reading/writing.

    Transparently opens compressed (``.gz``, ``.zst``) files.
    """
    if 'b' not in mode and 't' not in mode:
        mode += 't'
    encoding = None if 'b' in mode else _TEXT_ENCODING
    if compression == Compression.GZIP.value:
        return cast(IO, gzip.open(path, mode, encoding=encoding))
    elif compression == Compression.ZSTD.value:
        #
        # zstandard does not support some operations for binary data (e.g. readline)
        # https://github.com/indygreg/python-zstandard/issues/136
        #
        reader = zstandard.open(path, mode, encoding=encoding)  # type: ignore
        if 'b' in mode:
            reader = io.BufferedReader(reader) if 'r' in mode else io.BufferedWriter(reader)
        return cast(IO, reader)
    else:
        raise ValueError("Unsupported compression format: %r" % compression)


def _is_valid_gzip_header(header: bytes) -> bool:
    #
    # Extra sanity checks to ensure that we are working with the actual gzip header.
    # There is a chance that compressed data may look like the start of gzip header.
    #
    if len(header) < _GZIP_HEADER_LENGTH:
        return False

    try:
        unix_timestamp = struct.unpack("i", header[4:8])[0]
    except Exception as err:
        _LOGGER.debug("Can't parse GZIP header: %s", err)
        return False

    if unix_timestamp < _OLDEST_UNIX_TIMESTAMP or unix_timestamp > time.time():
        return False

    if header[9] not in _POSSIBLE_OS_TYPES:
        return False

    flags = header[4]
    if flags > 0xfc:
        return True

    return True


def _is_valid_zstd_header(header: bytes) -> bool:
    flags = header[4:5]
    binary_string = "{:08b}".format(int(flags.hex(), 16))
    if not binary_string[3:5] == '00':
        return False
    if not binary_string[5] == '1':
        return False
    return True


def _iterate_archives(
    fin: IO[bytes],
    buffer_size: int = _MIN_CHUNK_SIZE,
    compression: Optional[str] = Compression.GZIP.value
) -> Iterable[Tuple[IO[bytes], int, int]]:
    #
    # We could use ByteIO container here, but byte strings work faster and easier
    # to work with for our particular case.
    #
    archive = b""
    start_offset, end_offset = 0, 0

    assert compression in SUPPORTED_COMPRESSIONS
    if compression == Compression.GZIP.value:
        w_offset, header, header_length = _WINDOW_OFFSET_GZIP, _GZIP_HEADER, _GZIP_HEADER_LENGTH
        valid_header = _is_valid_gzip_header
    else:
        w_offset, header, header_length = _WINDOW_OFFSET_ZST, _ZSTD_HEADER, _ZSTD_HEADER_LENGTH
        valid_header = _is_valid_zstd_header

    while True:
        chunk = fin.read(buffer_size)

        if not chunk:
            start_offset = end_offset
            end_offset = start_offset + len(archive)

            yield io.BytesIO(archive), start_offset, end_offset
            return

        #
        # Include data from the previous chunk to be sure that gzip/zstd header won't be splitted
        # across multiple chunks.
        #
        window = archive[w_offset:] + chunk
        archive += chunk
        if len(window) < header_length or window.rfind(header) == -1:
            continue

        header_pos = archive.rfind(header)
        if any([
            header_pos <= 0,
            len(archive) < header_length,
            len(archive) - header_pos < header_length,
            not valid_header(archive[header_pos:header_pos + header_length])
        ]):
            continue

        new_archive = archive[header_pos:]
        archive = archive[0:header_pos]
        start_offset = end_offset
        end_offset = start_offset + len(archive)
        yield io.BytesIO(archive), start_offset, end_offset
        archive = new_archive


def index_csv_file(
    csv_file: IO[bytes],
    output_file: IO[bytes],
    column: int = DEFAULT_CSV_COLUMN,
    delimiter: str = DEFAULT_CSV_DELIMITER,
    min_chunk_size: int = _MIN_CHUNK_SIZE
) -> None:
    """Index a compressed CSV file from the file stream.

    :param stream csv_file: The binary file stream to read input from.
    :param stream output_file: The binary file stream to write output to.
    :param int column: The index of the key column in the input file.
    :param str delimiter: The CSV delimiter to use.
    :param int min_chunk_size: The minimum number of bytes in a single chunk.
    """
    compression = _determine_compression_from_header(csv_file)
    chunk_iterator = _iterate_archives(
        fin=csv_file,
        buffer_size=min_chunk_size,
        compression=compression
    )
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        line_start, line_end = 0, 0
        with _open_compressed_file(arch, compression, mode='rb') as fin:
            csv_in = _StreamWrapper(fin, decode_lines=True)
            csv_reader = csv.reader(csv_in, delimiter=delimiter)
            for row in csv_reader:
                line_start = line_end
                line_end = line_start + len(csv_in.current_line)
                index = '%s|%s|%s|%s|%s' % (
                    row[column], start_offset,
                    end_offset - start_offset,
                    line_start, line_end - line_start,
                )
                output_file.write(index.encode(_TEXT_ENCODING) + _LINE_TERMINATOR)


def index_json_file(
    json_file: IO[bytes],
    output_file: IO[bytes],
    field: str = DEFAULT_JSON_FIELD,
    min_chunk_size: int = _MIN_CHUNK_SIZE
) -> None:
    """Index a compressed JSON file from the file stream.

    :param json_file: The binary file stream to read input from.
    :param output_file: The binary file stream to write output to.
    :param field: The name of the key field in the JSON file.
    :param min_chunk_size: The minimum number of bytes in a single chunk.
    """
    compression = _determine_compression_from_header(json_file)
    chunk_iterator = _iterate_archives(
        fin=json_file,
        buffer_size=min_chunk_size,
        compression=compression,
    )
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        line_start, line_end = 0, 0
        with _open_compressed_file(arch, compression, mode='rb') as json_in:
            for line in json_in:
                data = json.loads(line.decode(_TEXT_ENCODING))
                line_start = line_end
                line_end = line_start + len(line)
                index = '%s|%s|%s|%s|%s' % (
                    data[field],
                    start_offset, end_offset - start_offset,
                    line_start, line_end - line_start,
                )
                output_file.write(index.encode(_TEXT_ENCODING) + _LINE_TERMINATOR)


def _batch_iterator(
    iterator: Iterable,
    decode_lines: bool = False,
    batch_size: int = _MAX_RECORDS_PER_BATCH
) -> Iterable[List[Any]]:
    items = []
    for item in iterator:
        if decode_lines:
            items.append(item.strip().decode(_TEXT_ENCODING))
        else:
            items.append(item)
        if len(items) == batch_size:
            yield items
            items = []
    if items:
        yield items


def _scan_index(keys: Iterable, index_fin: IO[str]) -> Dict[int, list]:
    #
    # Groups indexes by gzip/zstd chunks and filters them.
    #
    keys_idx = collections.defaultdict(list)
    keys = set(keys)
    keys_seen = set()
    csv_reader = csv.reader(index_fin, delimiter=DEFAULT_CSV_DELIMITER)
    for row in csv_reader:
        key, start_offset = row[0], int(row[1])
        if key in keys:
            keys_idx[start_offset].append(row)
            keys_seen.add(key)

    missing_keys = keys - keys_seen
    if missing_keys:
        _LOGGER.error("Missing keys: %r" % missing_keys)
    return keys_idx


class _StreamWrapper:
    """A custom wrapper that keeps a reference to the current line."""

    def __init__(self, fin, decode_lines=False):
        self.f = fin
        self.encode_line = decode_lines
        self.current_line = None

    def __iter__(self):
        return self

    def __next__(self):
        self.current_line = next(self.f)
        if self.encode_line:
            return self.current_line.decode(_TEXT_ENCODING)
        else:
            return self.current_line


def retrieve(
    keys_fin: IO[bytes], file_path: str, index_fin: IO[str], output_stream: IO[bytes]
) -> None:
    """Retrieve data from an indexed file.

    :param keys_fin: A steam with list of keys to retrieve.
    :param file_path: A local S3 path to the file retrieve data from.
    :param index_fin: A file stream to read index from.
    :param output_stream: A file stream to output results to.
    """
    compression = _get_compression_type(file_path)
    input_fin = smart_open.open(file_path, 'rb', ignore_ext=True)
    for keys in _batch_iterator(keys_fin, decode_lines=True):
        keys_idx = _scan_index(keys, index_fin)
        displayed = set()
        for group in keys_idx.values():
            index = group[0]
            start_offset, offset_length = int(index[1]), int(index[2])
            input_fin.seek(start_offset)

            compressed_chunk = io.BytesIO(input_fin.read(offset_length))
            with _open_compressed_file(compressed_chunk, compression, mode='rb') as fin:
                for row in group:
                    fin.seek(int(row[3]))
                    domain = row[0]
                    line = fin.read(int(row[4]))
                    output_stream.write(line)
                    if domain in displayed:
                        _LOGGER.error("multiple matches for %s key")
                    displayed.add(domain)


def _start_of_line(
    fin: IO[bytes],
    lineterminator: bytes = _LINE_TERMINATOR,
    bufsize: int = io.DEFAULT_BUFFER_SIZE
) -> None:
    """Moves the file pointer back to the start of the current line."""
    while True:
        current_pos = fin.tell()
        if current_pos == 0:
            break

        seek_pos = max(0, current_pos - bufsize)
        fin.seek(seek_pos)
        buf = fin.read(current_pos - seek_pos)

        assert fin.tell() == current_pos, 'we should be back at current_pos'

        if lineterminator in buf:
            index = max([i for (i, c) in enumerate(buf) if c == ord(lineterminator)])
            fin.seek(seek_pos + index + 1)
            break
        elif bufsize < current_pos:
            #
            # Try again with a larger lookbehind buffer.
            #
            bufsize *= 2
        else:
            fin.seek(0)
            break


def _buffer_chunk(
    fin: IO[bytes],
    start: int,
    end: int,
    pivot: int,
    lineterminator: bytes,
) -> Tuple[IO[bytes], int, int, int]:
    #
    # This function reads a specified chunk into memory to avoid hitting disk or network
    # storage on every seek/read call. We use it when the search scope is relatively small.
    #
    # When buffered, the binary search algorithm won't have access to the rest of the file.
    # Because of that, it's important to ensure that we keep complete lines at the start
    # and end of the chunk.
    #

    fin.seek(start)
    _start_of_line(fin)
    left_shift = start - fin.tell()
    size = (end - start) + left_shift
    buf = io.BytesIO(fin.read(size))

    buf.seek(-1, io.SEEK_END)
    if buf.read() != lineterminator:
        buf.seek(0, io.SEEK_END)
        buf.write(fin.readline())

    size = buf.getbuffer().nbytes
    pivot = pivot - start + left_shift
    start, end = 0, size

    buf.seek(0)
    return buf, start, end, pivot


def _is_last_line(fin: IO[bytes], start: int, end: int) -> bool:
    current_pos = fin.tell()
    fin.seek(start)
    data = fin.read(end - start)
    #
    # It's possible to be in the middle of two last lines.
    #
    if data.count(_LINE_TERMINATOR) == 0:
        return True
    else:
        fin.seek(current_pos)
        return False


def _binary_search(
    key: bytes,
    fin: IO[bytes],
    fsize: int,
    delimiter: bytes = DEFAULT_CSV_DELIMITER.encode(_TEXT_ENCODING),
    lineterminator: bytes = _LINE_TERMINATOR,
    buffer_size: int = _DEFAULT_BUFFER_SIZE
) -> Optional[List[bytes]]:
    seen = set()
    start, pivot, end = 0, fsize // 2, fsize
    buffered = False

    if fsize < buffer_size * _BYTES_IN_KiB:
        fin = io.BytesIO(fin.read())
        buffered = True

    while True:
        #
        # The assertion will trip if there is a bug in the code, or if the
        # index isn't sorted properly.
        #
        assert (start, pivot, end) not in seen, 'stuck in an infinite loop'
        seen.add((start, pivot, end))

        fin.seek(pivot)
        _start_of_line(fin)

        line = fin.readline()

        candidate, rest = line.split(delimiter, 1)
        _LOGGER.debug(
            'start: %r pivot: %r end: %r candidate: %r',
            start, pivot, end, candidate,
        )

        if candidate == key:
            return rest.rstrip(lineterminator).split(delimiter)
        elif fin.tell() == fsize:
            #
            # Reached EOF
            #
            raise KeyError(key)
        elif buffered and fin.tell() > end and _is_last_line(fin, start, end):
            raise KeyError(key)
        elif key < candidate:
            start, pivot, end = start, (pivot + start) // 2, pivot
        else:
            start, pivot, end = pivot, (pivot + end) // 2, end

        if not buffered and end - start < buffer_size * _BYTES_IN_KiB:
            #
            # Download the entire search scope to an internal in-memory buffer to reduce the number
            # of disk or network seek/read operations.
            #
            fin, start, end, pivot = _buffer_chunk(fin, start, end, pivot, lineterminator)
            buffered = True


def _getsize(path: str, transport_params: Optional[dict]) -> int:
    """Return the size of an file-like object, in bytes.

    Works for both S3 and local objects.
    """
    with smart_open.open(path, 'rb', ignore_ext=True, transport_params=transport_params) as fin:
        fin.seek(0, io.SEEK_END)
        return fin.tell()


def search(
    key: bytes,
    file_path: str,
    index_path: str,
    output_stream: IO[bytes],
    buffer_size: int = _DEFAULT_BUFFER_SIZE,
    transport_params: Optional[dict] = None
) -> None:
    """Look up a single key in the index, and retrieve the corresponding line.

    :param key: The key to search for.
    :param file_path: A local or S3 path to the file retrieve data from.
    :param index_path: A local or S3 path to the index file.
    :param output_stream: The stream to output result to.
    :param buffer_size: The maximum size of the index file chunk to load in memory, in KiB.
    :param transport_params: Optional parameters for reading the files remotely.
    """
    try:
        fsize = _getsize(index_path, transport_params=transport_params) # type: ignore
    except (FileNotFoundError, botocore.exceptions.BotoCoreError) as err:
        _LOGGER.error("Can't open index file: %s", err)
        sys.exit(1)

    with smart_open.open(index_path, 'rb', transport_params=transport_params) as fin:
        chunk_offset, chunk_len, line_offset, line_len = _binary_search(
            key, fin, fsize, buffer_size=buffer_size
        )  # type: ignore

    chunk_offset = int(chunk_offset)
    chunk_len = int(chunk_len)
    line_offset = int(line_offset)
    line_len = int(line_len)

    try:
        fin = smart_open.open(file_path, 'rb', ignore_ext=True, transport_params=transport_params)
    except (FileNotFoundError, botocore.exceptions.BotoCoreError) as err:
        _LOGGER.error("Can't open data file: %s", err)
        sys.exit(1)

    with fin:
        fin.seek(chunk_offset)
        chunk = io.BytesIO(fin.read(chunk_len))
        compression = _get_compression_type(file_path)
        with _open_compressed_file(chunk, compression, mode='rb') as inner_fin:
            inner_fin.seek(line_offset)
            output_stream.write(inner_fin.read(line_len))


def _extract_keys_from_json(line: bytes, field: str) -> str:
    data = json.loads(line.decode(_TEXT_ENCODING))
    return data[field]


def _extract_keys_from_csv(line: bytes, column: str, delimiter: str) -> str:
    reader = csv.reader(io.StringIO(line.decode(_TEXT_ENCODING)), delimiter=delimiter)
    return next(reader)[column]  # type: ignore


def _repack(
    fin: IO[bytes],
    fout: IO[bytes],
    index_fout: IO[bytes],
    chunk_size: int,
    extractor: Callable,
    output_compression: str = Compression.GZIP.value,
) -> None:
    start_offset, end_offset = 0, 0
    compressed_chunk = None
    compression = _determine_compression_from_header(fin)
    assert compression in SUPPORTED_COMPRESSIONS
    assert output_compression in SUPPORTED_COMPRESSIONS

    with _open_compressed_file(fin, compression, mode='rb') as fin:
        for batch in _batch_iterator(fin, decode_lines=False, batch_size=chunk_size):
            keys = []
            line_indexes = []
            chunk = io.BytesIO()
            compressed_chunk = _open_compressed_file(chunk, output_compression, mode='wb')

            line_start, line_end = 0, 0
            for line in batch:
                key = extractor(line)
                keys.append(key)
                line_start = line_end
                line_end = line_start + len(line)
                line_indexes.append('|%s|%s' % (line_start, line_end - line_start))
                compressed_chunk.write(line)

            compressed_chunk.close()
            fout.write(chunk.getvalue())
            fout.flush()

            start_offset = end_offset
            end_offset = start_offset + chunk.getbuffer().nbytes
            for i, key in enumerate(keys):
                index = '%s|%s|%s' % (key, start_offset, end_offset - start_offset)
                index += line_indexes[i]
                index_fout.write(index.encode(_TEXT_ENCODING) + _LINE_TERMINATOR)
            index_fout.flush()

    if compressed_chunk is None:
        #
        # The input file contained no data.  We must write an empty gzip chunk
        # to make sure the output file is gzip-readable.
        #
        fout = io.BytesIO()
        zstandard.open(fout, mode='wb').write(b'').close()
        fout.write(fout.getvalue())
        fout.flush()


def sort_file(file_path: str) -> None:
    """Sort a file using GNU toolchain.

    :param file_path: The path to file to sort.
    """
    sorted_handle, tmp_path = tempfile.mkstemp(prefix='gzipi')
    os.close(sorted_handle)
    shutil.move(file_path, tmp_path)

    #
    # We use sort from GNU toolchain here, because index file can be pretty big.
    #
    sort_flags = [
        '--field-separator=|',
        '--key=1,1',
        '--parallel=%s' % _SORT_CPU_COUNT,
        '--buffer-size=%s' % _SORT_BUFFER_SIZE
    ]
    gzcat = plumbum.local[get_exe('gzcat', 'zcat')][tmp_path]
    cat = plumbum.local['cat'][tmp_path]
    gzip_exe = plumbum.local['gzip']['--stdout']
    sort = plumbum.local[get_exe('gsort', 'sort')][sort_flags]

    file_path = os.path.abspath(file_path)
    is_gzipped = file_path.endswith('.gz')

    with plumbum.local.env(LC_ALL='C'):
        try:
            if is_gzipped:
                return ((gzcat | sort | gzip_exe) > file_path) & plumbum.FG
            else:
                return ((cat | sort) > file_path)()
        finally:
            os.remove(tmp_path)


def repack_json_file(
    fin: IO[bytes],
    fout: IO[bytes],
    index_fout: IO[bytes],
    chunk_size: int,
    field: str = DEFAULT_JSON_FIELD,
    output_compression: str = Compression.GZIP.value,
) -> None:
    """Repack a JSON file.

    :param fin: A compressed binary file stream to read from.  Must contain JSON.
    :param fout: A compressed binary file stream to write to.
    :param index_fout: A **text** file stream to write the index to.
    :param chunk_size: The number of lines to include in each chunk.
    :param field: The field to use when creating the index.
    :param output_compression: The compression format of the output file. Supports gzip and zstd.
    """
    extractor = functools.partial(_extract_keys_from_json, field=field)
    return _repack(fin, fout, index_fout, chunk_size, extractor, output_compression)


def repack_csv_file(
    fin: IO[bytes],
    fout: IO[bytes],
    index_fout: IO[bytes],
    chunk_size: int,
    column: int = DEFAULT_CSV_COLUMN,
    delimiter: str = DEFAULT_CSV_DELIMITER,
    output_compression: str = Compression.GZIP.value,
) -> None:
    """Repack a CSV file.

    :param fin: A compressed binary file stream to read from.  Must contain CSV.
    :param fout: A compressed binary file stream to write to.
    :param index_fout: A **text** file stream to write the index to.
    :param chunk_size: The number of lines to include in each chunk.
    :param column: The index of the column to use when creating the index.
    :param delimiter: The CSV column delimiter.
    :param output_compression: The compression format of the output file. Supports gzip and zstd.
    """
    extractor = functools.partial(_extract_keys_from_csv, column=column, delimiter=delimiter)
    return _repack(fin, fout, index_fout, chunk_size, extractor, output_compression)


def get_exe(*preference) -> Optional[str]:
    """Return the path to the full executable, given a list of candidates.

    The list should be in order of decreasing preference.
    """
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path
    return None
