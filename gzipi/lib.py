#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
"""Main module that implements gzip indexing and searching.

The main entry points are:

- index_csv_file, index_json_file: Scan a file and create a new index file.
- retrieve_from_csv,retrieve_from_json: Use a previously created index to quickly
 access individual lines in the compressed file.
- repack_json_file, repack_csv_file: Recompress a gzip file and create a new index for it.

Type ``gzipi --help`` in the terminal for more information and CLI examples.
"""

import collections
import csv
import functools
import gzip
import io
import json
import logging
import struct
import time
import urllib.parse

import boto3

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
_GZIP_HEADER_LENGHT = 10
"""The length of main fields of gzip header, in bytes."""
_WINDOW_OFFSET = (_GZIP_HEADER_LENGHT - 1) * -1
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

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())


def _is_valid_gzip_header(gzip_header):
    #
    # Extra sanity checks to ensure that we are working with the actual gzip header.
    # There is a chance that compressed data may look like the start of gzip header.
    #
    if len(gzip_header) < _GZIP_HEADER_LENGHT:
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
        if len(window) < _GZIP_HEADER_LENGHT or window.rfind(_GZIP_HEADER) == -1:
            continue

        header_pos = archive.rfind(_GZIP_HEADER)
        if any([
            header_pos <= 0,
            len(archive) < _GZIP_HEADER_LENGHT,
            len(archive) - header_pos < _GZIP_HEADER_LENGHT,
            not _is_valid_gzip_header(archive[header_pos:header_pos + _GZIP_HEADER_LENGHT])
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
    delimiter=DEFAULT_CSV_DELIMITER, index_lines=True, min_chunk_size=_MIN_CHUNK_SIZE
):
    """Index a CSV file from the file stream.

    Possible formats of the index file:

    key|gzip_start_offset|gzip_length
    key|gzip_start_offset|gzip_length|line_start_offset|line_length (when index_lines is True)

    All offsets are in bytes.

    :param stream csv_file: The binary file stream to read input from.
    :param stream output_file: The binary file stream to write output to.
    :param int column: The index of the key column in the input file.
    :param str delimiter: The CSV delimiter to use.
    :param bool index_lines: If True, indexes lines inside gzip chunks as well.
    :param int min_chunk_size: The minimum number of bytes in a single gzip chunk.
    """
    chunk_iterator = _iterate_archives(csv_file, buffer_size=min_chunk_size)
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        if index_lines:
            line_start, line_end = 0, 0
        with gzip.open(arch, mode='rb') as fin:
            csv_in = _StreamWrapper(fin, decode_lines=True)
            csv_reader = csv.reader(csv_in, delimiter=delimiter)
            for row in csv_reader:
                index = '%s|%s|%s' % (row[column], start_offset, end_offset - start_offset)
                if index_lines:
                    line_start = line_end
                    line_end = line_start + len(csv_in.current_line)
                    index += '|%s|%s' % (line_start, line_end - line_start)
                output_file.write(index.encode(_TEXT_ENCODING) + b'\n')


def index_json_file(json_file, output_file, field=DEFAULT_JSON_FIELD, index_lines=True,
                    min_chunk_size=_MIN_CHUNK_SIZE):
    """Index a JSON file from the file stream.

    Possible formats of the index file:

    key|gzip_start_offset|gzip_length
    key|gzip_start_offset|gzip_length|line_start_offset|line_length (when index_lines is True)

    All offsets are in bytes.

    :param stream json_file: The binary file stream to read input from.
    :param stream output_file: The binary file stream to write output to.
    :param str field: The name of the key field in the JSON file.
    :param bool index_lines: If True, indexes lines inside gzip chunks as well.
    :param int min_chunk_size: The minimum number of bytes in a single gzip chunk.
    """
    chunk_iterator = _iterate_archives(json_file, buffer_size=min_chunk_size)
    for i, (arch, start_offset, end_offset) in enumerate(chunk_iterator):
        _LOGGER.info('processed %s chunk, offset: %s-%s' % (i, start_offset, end_offset))
        if index_lines:
            line_start, line_end = 0, 0
        with gzip.open(arch, mode='rb') as json_in:
            for line in json_in:
                data = json.loads(line.decode(_TEXT_ENCODING))
                index = '%s|%s|%s' % (data[field], start_offset, end_offset - start_offset)
                if index_lines:
                    line_start = line_end
                    line_end = line_start + len(line)
                    index += '|%s|%s' % (line_start, line_end - line_start)
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
    csv_reader = csv.reader(index_fin, delimiter=DEFAULT_CSV_DELIMITER)
    for row in csv_reader:
        key, start_offset = row[0], int(row[1])
        if key in keys:
            keys_idx[start_offset].append(row)
    return keys_idx


def _scan_json(index, fin, field, multiindex=False):
    chunk_data = {}
    if multiindex:
        for row in index:
            fin.seek(int(row[3]))
            line = fin.read(int(row[4])).decode(_TEXT_ENCODING)
            data = json.loads(line)
            chunk_data[data[field]] = line
    else:
        keys = set([r[0] for r in index])
        for line in fin:
            data = json.loads(line)
            if data[field] in keys:
                chunk_data[data[field]] = line.decode(_TEXT_ENCODING)
            if len(chunk_data) == len(keys):
                break
    return chunk_data


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


def _scan_csv(index, fin, column, delimiter, multiindex=False):
    chunk_data = {}

    if multiindex:
        for row in index:
            fin.seek(int(row[3]))
            line = fin.read(int(row[4])).decode(_TEXT_ENCODING)
            record = next(csv.reader(io.StringIO(line), delimiter=delimiter))
            chunk_data[record[column]] = line
    else:
        #
        # We use a custom wrapper here, because it's not possible to iterate
        # over raw and parsed strings at the same time using csv.reader.
        #
        fin = _StreamWrapper(fin, decode_lines=True)
        csv_reader = csv.reader(fin, delimiter=delimiter)
        keys = set([r[0] for r in index])
        for row in csv_reader:
            if row[column] in keys:
                chunk_data[row[column]] = fin.current_line.decode(_TEXT_ENCODING)
            if len(chunk_data) == len(index):
                break
    return chunk_data


def _retrieve_from_local_path(keys_idx, fin, reader):
    batch_data = {}
    for group in keys_idx.values():
        index = group[0]
        start_offset, offset_length = int(index[1]), int(index[2])
        fin.seek(start_offset)
        gzip_chunk = io.BytesIO()
        gzip_chunk.write(fin.read(offset_length))
        gzip_chunk.seek(0)
        with gzip.open(gzip_chunk, 'rb') as gzip_in:
            batch_data.update(reader(group, gzip_in))
    return batch_data


def _parse_s3_url(uri):
    uri = urllib.parse.urlparse(uri)
    if uri.scheme != 's3':
        raise ValueError("Unrecognized URI scheme: %s" % uri.scheme)
    return uri.netloc, uri.path.lstrip('/')


def _retrieve_from_s3(keys_idx, s3_path, reader):
    bucket, key = _parse_s3_url(s3_path)
    obj = boto3.resource('s3').Object(bucket, key)
    batch_data = {}
    for group in keys_idx.values():
        index = group[0]
        start_offset, offset_length = int(index[1]), int(index[2])
        byte_range = 'bytes=%s-%s' % (start_offset, start_offset + offset_length - 1)
        stream = obj.get(Range=byte_range)['Body']
        gzip_chunk = io.BytesIO()
        gzip_chunk.write(stream.read())
        gzip_chunk.seek(0)
        with gzip.open(gzip_chunk, 'rb') as gzip_fin:
            batch_data.update(reader(group, gzip_fin))
    return batch_data


def _is_multiindex(fin):
    n_cols = fin.readline().count(DEFAULT_CSV_DELIMITER) + 1
    fin.seek(0)
    return n_cols == 5


def _retrieve(keys, file_path, index_fin, output_stream, reader):
    if file_path.startswith('s3://'):
        retrieve, input_fin = _retrieve_from_s3, file_path
    else:
        retrieve, input_fin = _retrieve_from_local_path, open(file_path, 'rb')

    for keys in _batch_iterator(keys, decode_lines=True):
        keys_idx = _scan_index(set(keys), index_fin)
        chunk_data = retrieve(keys_idx, input_fin, reader)
        for key in keys:
            if key in chunk_data:
                output_stream.write(chunk_data[key].encode(_TEXT_ENCODING))
            else:
                output_stream.write(b'\n')


def retrieve_from_csv(keys_fin, csv_path, index_path, output_stream, column=DEFAULT_CSV_COLUMN,
                      delimiter=DEFAULT_CSV_DELIMITER):
    """Retrieve data from indexed CSV file.

    :param file keys_fin: A steam with list of keys to retrieve.
    :param str csv_path: A local S3 path to the file retrieve data from.
    :param str  index_path: A file stream to read index from.
    :param file output_stream: A file stream to output results to.
    :param int column: The index of the key column in the input file.
    :param str delimiter: The CSV delimiter to use.
    """
    index_fin = gzip.open(index_path, 'rt')
    multiindex = _is_multiindex(index_fin)
    reader = functools.partial(_scan_csv, column=column, delimiter=delimiter, multiindex=multiindex)
    return _retrieve(keys_fin, csv_path, index_fin, output_stream, reader)


def retrieve_from_json(keys_fin, json_path, index_path, output_stream, field=DEFAULT_JSON_FIELD):
    """Retrieve data from indexed JSON file.

    :param file keys_fin: A steam with list of keys to retrieve.
    :param str json_path: A local S3 path to the file retrieve data from.
    :param str  index_path: A file stream to read index from.
    :param file output_stream: A file stream to output results to.
    :param str field: The name of the key field in the JSON file.
    """

    index_fin = gzip.open(index_path, 'rt')
    multiindex = _is_multiindex(index_fin)
    reader = functools.partial(_scan_json, field=field, multiindex=multiindex)
    return _retrieve(keys_fin, json_path, index_fin, output_stream, reader)


def _extract_keys_from_json(line, field):
    data = json.loads(line.decode(_TEXT_ENCODING))
    return data[field]


def _extract_keys_from_csv(line, column, delimiter):
    reader = csv.reader(io.StringIO(line.decode(_TEXT_ENCODING)), delimiter=delimiter)
    return next(reader)[column]


def _repack(fin, fout, index_fout, chunk_size, extractor, index_lines=True):
    start_offset, end_offset = 0, 0
    with gzip.open(fin, 'rb') as fin:
        for batch in _batch_iterator(fin, decode_lines=False, batch_size=chunk_size):
            keys = []
            if index_lines:
                line_indexes = []
            chunk = io.BytesIO()
            gzipped_chunk = gzip.GzipFile(fileobj=chunk, mode='wb')

            if index_lines:
                line_start, line_end = 0, 0

            for line in batch:
                key = extractor(line)
                keys.append(key)
                if index_lines:
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
                if index_lines:
                    index += line_indexes[i]
                index_fout.write(index.encode(_TEXT_ENCODING) + b'\n')
            index_fout.flush()


def repack_json_file(fin, fout, index_fout, chunk_size, field=DEFAULT_JSON_FIELD):
    extractor = functools.partial(_extract_keys_from_json, field=field)
    return _repack(fin, fout, index_fout, chunk_size, extractor)


def repack_csv_file(fin, fout, index_fout, chunk_size, column=DEFAULT_CSV_COLUMN,
                    delimiter=DEFAULT_CSV_DELIMITER):
    extractor = functools.partial(_extract_keys_from_csv, column=column, delimiter=delimiter)
    return _repack(fin, fout, index_fout, chunk_size, extractor)
