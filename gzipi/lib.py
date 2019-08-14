#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
"""Main module that implements gzip indexing and searching.

The main entry points are:

- index_csv_file, index_json_file: Scan a file and create a new index file.
- retrieve: Use a previously created index to quickly
  access individual lines in the compressed file.
- repack_json_file, repack_csv_file: Recompress a gzip file and create a new index for it.

Type ``gzipi --help`` in the terminal for more information and CLI examples.
"""

import collections
import csv
import distutils.spawn
import functools
import gzip
import io
import json
import logging
import multiprocessing
import os
import os.path as P
import shutil
import struct
import sys
import tempfile
import time

import boto3
import smart_open
import plumbum

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
_GZIP_HEADER_LENGTH = 10
"""The length of main fields of gzip header, in bytes."""
_WINDOW_OFFSET = (_GZIP_HEADER_LENGTH - 1) * -1
_MIN_CHUNK_SIZE = 100000
"""The minimum amount of bytes in each chunk.

If this value is larger than the actual minimum size, it's possible that two chunks
will be joined into one.
"""
FILE_FORMATS = ('csv', 'json')
"""Supported file formats."""
DEFAULT_CSV_COLUMN = 0
DEFAULT_CSV_DELIMITER = '|'
DEFAULT_JSON_FIELD = 'domain'

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


def _is_valid_gzip_header(gzip_header):
    #
    # Extra sanity checks to ensure that we are working with the actual gzip header.
    # There is a chance that compressed data may look like the start of gzip header.
    #
    if len(gzip_header) < _GZIP_HEADER_LENGTH:
        return False

    try:
        unix_timestamp = struct.unpack("i", gzip_header[4:8])[0]
    except Exception as err:
        _LOGGER.debug("Can't parse GZIP header: %s", err)
        return False

    if unix_timestamp < _OLDEST_UNIX_TIMESTAMP or unix_timestamp > time.time():
        return False

    if gzip_header[9] not in _POSSIBLE_OS_TYPES:
        return False

    flags = gzip_header[4]
    if flags > 0xfc:
        return True

    return True


def _iterate_archives(fin, buffer_size=_MIN_CHUNK_SIZE):
    #
    # We could use ByteIO container here, but byte strings work faster and easier
    # to work with for our particular case.
    #
    archive = b""
    start_offset, end_offset = 0, 0

    while True:
        chunk = fin.read(buffer_size)

        if not chunk:
            start_offset = end_offset
            end_offset = start_offset + len(archive)

            yield io.BytesIO(archive), start_offset, end_offset
            return

        #
        # Include data from the previous chunk to be sure that gzip header won't be splitted
        # across multiple chunks.
        #
        window = archive[_WINDOW_OFFSET:] + chunk
        archive += chunk
        if len(window) < _GZIP_HEADER_LENGTH or window.rfind(_GZIP_HEADER) == -1:
            continue

        header_pos = archive.rfind(_GZIP_HEADER)
        if any([
            header_pos <= 0,
            len(archive) < _GZIP_HEADER_LENGTH,
            len(archive) - header_pos < _GZIP_HEADER_LENGTH,
            not _is_valid_gzip_header(archive[header_pos:header_pos + _GZIP_HEADER_LENGTH])
        ]):
            continue

        new_archive = archive[header_pos:]
        archive = archive[0:header_pos]
        start_offset = end_offset
        end_offset = start_offset + len(archive)
        yield io.BytesIO(archive), start_offset, end_offset
        archive = new_archive


def index_csv_file(
    csv_file, output_file, column=DEFAULT_CSV_COLUMN,
    delimiter=DEFAULT_CSV_DELIMITER, min_chunk_size=_MIN_CHUNK_SIZE
):
    """Index a gzipped CSV file from the file stream.

    The format of the index will be::

        key|gzip_start_offset|gzip_length|line_start_offset|line_length

    All offsets are in bytes.

    :param stream csv_file: The binary file stream to read input from.
    :param stream output_file: The binary file stream to write output to.
    :param int column: The index of the key column in the input file.
    :param str delimiter: The CSV delimiter to use.
    :param int min_chunk_size: The minimum number of bytes in a single gzip chunk.
    """
    chunk_iterator = _iterate_archives(csv_file, buffer_size=min_chunk_size)
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        line_start, line_end = 0, 0
        with gzip.open(arch, mode='rb') as fin:
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
                output_file.write(index.encode(_TEXT_ENCODING) + b'\n')


def index_json_file(json_file, output_file, field=DEFAULT_JSON_FIELD,
                    min_chunk_size=_MIN_CHUNK_SIZE):
    """Index a gzipped JSON file from the file stream.

    Format of the index file:

        key|gzip_start_offset|gzip_length|line_start_offset|line_length

    All offsets are in bytes.

    :param stream json_file: The binary file stream to read input from.
    :param stream output_file: The binary file stream to write output to.
    :param str field: The name of the key field in the JSON file.
    :param int min_chunk_size: The minimum number of bytes in a single gzip chunk.
    """
    chunk_iterator = _iterate_archives(json_file, buffer_size=min_chunk_size)
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        line_start, line_end = 0, 0
        with gzip.open(arch, mode='rb') as json_in:
            for line in json_in:
                data = json.loads(line.decode(_TEXT_ENCODING))
                line_start = line_end
                line_end = line_start + len(line)
                index = '%s|%s|%s|%s|%s' % (
                    data[field],
                    start_offset, end_offset - start_offset,
                    line_start, line_end - line_start,
                )
                output_file.write(index.encode(_TEXT_ENCODING) + b'\n')


def _batch_iterator(iterator, decode_lines=False, batch_size=_MAX_RECORDS_PER_BATCH):
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


def _scan_index(keys, index_fin):
    #
    # Groups indexes by gzip chunks and filters them.
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


def retrieve(keys_fin, file_path, index_fin, output_stream):
    """Retrieve data from an indexed file.

    :param file keys_fin: A steam with list of keys to retrieve.
    :param str file_path: A local S3 path to the file retrieve data from.
    :param str index_fin: A file stream to read index from.
    :param file output_stream: A file stream to output results to.
    """

    input_fin = smart_open.open(file_path, 'rb', ignore_ext=True)
    for keys in _batch_iterator(keys_fin, decode_lines=True):
        keys_idx = _scan_index(keys, index_fin)
        displayed = set()
        for group in keys_idx.values():
            index = group[0]
            start_offset, offset_length = int(index[1]), int(index[2])
            input_fin.seek(start_offset)

            gzip_chunk = io.BytesIO(input_fin.read(offset_length))
            with gzip.open(gzip_chunk, 'rb') as gzip_fin:
                for row in group:
                    gzip_fin.seek(int(row[3]))
                    domain = row[0]
                    line = gzip_fin.read(int(row[4]))
                    output_stream.write(line)
                    if domain in displayed:
                        _LOGGER.error("multiple matches for %s key")
                    displayed.add(domain)


def _start_of_line(fin, lineterminator=b'\n', bufsize=io.DEFAULT_BUFFER_SIZE):
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


def _binary_search(key, fin, fsize, delimiter=b'|', lineterminator=b'\n'):
    seen = set()
    start, pivot, end = 0, fsize // 2, fsize

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
        elif key < candidate:
            start, pivot, end = start, (pivot + start) // 2, pivot
        else:
            start, pivot, end = pivot, (pivot + end) // 2, end


def _getsize(path):
    """Return the size of an object, in bytes.

    Works for both S3 and local objects.
    """
    if path.startswith('s3://'):
        parsed_uri = smart_open.smart_open_lib._parse_uri(path)

        session = boto3.Session()
        s3 = session.resource('s3')
        obj = s3.Object(parsed_uri.bucket_id, parsed_uri.key_id)
        return obj.get()['ContentLength']
    else:
        return P.getsize(path)


def search(key, file_path, index_path, output_stream):
    """Look up a single key in the index, and retrieve the corresponding line.

    :param bytes key: The key to search for.
    :param str file_path: A local or S3 path to the file retrieve data from.
    :param str index_path: A local or S3 path to the index file.
    :param str output_stream: The stream to output result to.
    """
    fsize = _getsize(index_path)
    with smart_open.open(index_path, 'rb') as fin:
        chunk_offset, chunk_len, line_offset, line_len = _binary_search(key, fin, fsize)

    chunk_offset = int(chunk_offset)
    chunk_len = int(chunk_len)
    line_offset = int(line_offset)
    line_len = int(line_len)

    with smart_open.open(file_path, 'rb', ignore_ext=True) as fin:
        fin.seek(chunk_offset)
        gzip_chunk = io.BytesIO(fin.read(chunk_len))
        with gzip.open(gzip_chunk, 'rb') as inner_fin:
            inner_fin.seek(line_offset)
            output_stream.write(inner_fin.read(line_len))


def _extract_keys_from_json(line, field):
    data = json.loads(line.decode(_TEXT_ENCODING))
    return data[field]


def _extract_keys_from_csv(line, column, delimiter):
    reader = csv.reader(io.StringIO(line.decode(_TEXT_ENCODING)), delimiter=delimiter)
    return next(reader)[column]


def _repack(fin, fout, index_fout, chunk_size, extractor):
    start_offset, end_offset = 0, 0
    with gzip.open(fin, 'rb') as fin:
        for batch in _batch_iterator(fin, decode_lines=False, batch_size=chunk_size):
            keys = []
            line_indexes = []
            chunk = io.BytesIO()
            gzipped_chunk = gzip.GzipFile(fileobj=chunk, mode='wb')

            line_start, line_end = 0, 0
            for line in batch:
                key = extractor(line)
                keys.append(key)
                line_start = line_end
                line_end = line_start + len(line)
                line_indexes.append('|%s|%s' % (line_start, line_end - line_start))
                gzipped_chunk.write(line)

            gzipped_chunk.close()
            fout.write(chunk.getvalue())
            fout.flush()

            start_offset = end_offset
            end_offset = start_offset + chunk.getbuffer().nbytes
            for i, key in enumerate(keys):
                index = '%s|%s|%s' % (key, start_offset, end_offset - start_offset)
                index += line_indexes[i]
                index_fout.write(index.encode(_TEXT_ENCODING) + b'\n')
            index_fout.flush()


def sort_file(file_path):
    """Sort a file using GNU toolchain.

    :param str file_path: The path to file to sort.
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


def repack_json_file(fin, fout, index_fout, chunk_size, field=DEFAULT_JSON_FIELD):
    extractor = functools.partial(_extract_keys_from_json, field=field)
    return _repack(fin, fout, index_fout, chunk_size, extractor)


def repack_csv_file(fin, fout, index_fout, chunk_size, column=DEFAULT_CSV_COLUMN,
                    delimiter=DEFAULT_CSV_DELIMITER):
    extractor = functools.partial(_extract_keys_from_csv, column=column, delimiter=delimiter)
    return _repack(fin, fout, index_fout, chunk_size, extractor)


def get_exe(*preference):
    """Return the path to the full executable, given a list of candidates.

    The list should be in order of decreasing preference.
    """
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path
