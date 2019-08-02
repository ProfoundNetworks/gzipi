#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
"""A command-line interface module.

Type ``gzipi --help`` in the terminal for more information.
"""

#
# NB. This module automatically discovers subcommands. To implement a new
# subcommand, create a _subcommand_parser function. For details, see the
# _create_main_parser function.
#

import argparse
import logging
import sys
import os.path as P
import smart_open

from . import lib

_LOGGER = logging.getLogger(__name__)
_BINARY_STDIN, _BINARY_STDOUT = sys.stdin.buffer, sys.stdout.buffer
_GZIPI_EXTENSION = '.gzi'
_CONSENT_STRINGS = ('y', 'yes')
_ENCODING = 'utf-8'
_CLI_DESCRIPTION = """gzipi  <command> [<args>]

Available commands:
    gzipi index     Scan a file to create a new index.
    gzipi retrieve  Use a previously created index to quickly access individual
                    lines in the compressed file.  Loads the index into memory.
    gzipi search    Use a previously created index to quickly access a single
                    line in the compressed file.  Performs a binary search on
                    the index, which must be sorted on the key.
    gzipi repack    Recompress a gzip file and create a new index for it.
"""


def _strip_extension(file_path):
    return file_path.replace('.gz', '').rsplit('.', 1)[0]


def _index_subparser(subparsers):
    desc = 'Scan a file to create a new index.'
    parser = subparsers.add_parser('index', description=desc, help=desc)
    parser.add_argument(
        '-i', '--input-file', required=False,
        help='The path to the file to index. If path is not specified, reads from stdin.'
    )
    parser.add_argument(
        '-o', '--index-file', required=False,
        help="The path to save gzipped output to."
    )
    parser.add_argument('--format', required=True, choices=lib.FILE_FORMATS,
                        help='The format of the input file.')
    parser.add_argument('--column', type=int, required=False, default=lib.DEFAULT_CSV_COLUMN,
                        help='The index of key column to use for CSV format.')
    parser.add_argument('--delimiter', type=str, required=False, default=lib.DEFAULT_CSV_DELIMITER,
                        help='The delimiter to use for CSV format.')
    parser.add_argument('--field', required=False, default=lib.DEFAULT_JSON_FIELD,
                        help='The name of key field to use for JSON format.')
    parser.set_defaults(function=_index)


def _index(args):
    if isinstance(args.input_file, str) and not _exists(args.input_file):
        _LOGGER.error("Input file does not exist: %s", args.input_file)
        _LOGGER.error("Aborting.")
        sys.exit(1)

    if args.input_file:
        fin = smart_open.open(args.input_file, 'rb', ignore_ext=True)
    else:
        fin = _BINARY_STDIN

    if args.index_file is None and args.input_file:
        args.index_file = _strip_extension(args.input_file) + _GZIPI_EXTENSION

    if not args.index_file:
        _LOGGER.error(
            "Can't determine path for index file. "
            "Please set it manually via --index-file parameter."
        )
        sys.exit(1)

    if args.index_file and _exists(args.index_file):
        response = input(
            "Output index path already exists: %s."
            " Do you want to overwrite it? y/n\n" % args.index_file
        )
        if response.lower() not in _CONSENT_STRINGS:
            _LOGGER.error("Aborting.")
            sys.exit(1)

    fout = smart_open.open(args.index_file, 'wb')

    if args.format == 'csv':
        lib.index_csv_file(csv_file=fin, output_file=fout, column=args.column,
                           delimiter=args.delimiter)
    else:
        lib.index_json_file(json_file=fin, output_file=fout, field=args.field)
    fout.close()
    lib.sort_file(args.index_file)


def _retrieve_subparser(subparsers):
    desc = 'Scan a file for a provided list of keys given an index file.'
    parser = subparsers.add_parser('retrieve', description=desc, help=desc)
    parser.add_argument(
        '-k', '--keys', required=False,
        help='The path to the key strings (e.g. domains) to scan the input file for. '
             'If path is not specified, reads from stdin.'
    )
    parser.add_argument('-f', '--input-file', required=True,
                        help='The path to input file to scan. May be a local path or an S3 path.')
    parser.add_argument('-i', '--index-file', required=False,
                        help='The local path to read index data from.')
    parser.add_argument('-o', '--output-file', required=False,
                        help='The path to save gzipped output to. By default, outputs to stdout.')
    parser.set_defaults(function=_retrieve)


def _retrieve(args):
    if not args.index_file:
        args.index_file = _strip_extension(args.input_file) + _GZIPI_EXTENSION

    input_file = args.input_file
    if input_file and not _exists(input_file):
        _LOGGER.error("Input file does not exist: %s", args.input_file)
        _LOGGER.error("Aborting.")
        sys.exit(1)

    if args.keys and not P.exists(args.keys):
        _LOGGER.error("Keys file does not exist: %s", args.input_file)
        _LOGGER.error("Aborting.")
        sys.exit(1)

    keys_fin = smart_open.open(args.keys, mode='rb') if args.keys else _BINARY_STDIN

    if args.output_file:
        fout = smart_open.open(args.output_file, mode='wb', ignore_ext=True)
    else:
        fout = _BINARY_STDOUT

    index_fin = smart_open.open(args.index_file, 'r')
    lib.retrieve(
        keys_fin=keys_fin, file_path=args.input_file,
        index_fin=index_fin, output_stream=fout,
    )


def _search_subparser(subparsers):
    desc = 'Look up a single key in the index, and retrieve the corresponding line'

    parser = subparsers.add_parser('search', description=desc, help=desc)
    parser.add_argument('-k', '--key', required=True, help='The key to look up')
    parser.add_argument('-f', '--input-file', required=True,
                        help='The path to input file to scan. May be a local path or an S3 path.')
    parser.add_argument('-i', '--index-file', required=False,
                        help='The local path to read index data from.')
    parser.add_argument('-o', '--output-file', required=False,
                        help='The path to save gzipped output to. By default, outputs to stdout.')
    parser.set_defaults(function=_search)


def _search(args):
    if not args.index_file:
        args.index_file = _strip_extension(args.input_file) + _GZIPI_EXTENSION

    fout = smart_open.open(args.output_file, mode='wb') if args.output_file else _BINARY_STDOUT

    key = args.key.encode(_ENCODING)
    lib.search(key, args.input_file, args.index_file, fout)


def _exists(path):
    if path.startswith('s3://'):
        try:
            with smart_open.open(path, 'rb', ignore_ext=True) as fin:
                fin.read(1)
        except IOError:
            return False
        else:
            return True
    else:
        return P.exists(path)


def _repack_subparser(subparsers):
    desc = 'Repack a gzipped file into a chunked gzipped file and an index file.'
    parser = subparsers.add_parser('repack', description=desc, help=desc)
    parser.add_argument(
        '-f', '--input-file', required=False,
        help='The path to the input file to repack. If file is not specified, reads from stdin.'
    )
    parser.add_argument('-o', '--output-file', required=False,
                        help='The path to save recompressed file to.')
    parser.add_argument('-i', '--index-file', required=False,
                        help='The path to save gzipped index to.')
    parser.add_argument('--format', required=True, choices=lib.FILE_FORMATS,
                        help='The format of the input file.')
    parser.add_argument('--column', type=int, required=False, default=lib.DEFAULT_CSV_COLUMN,
                        help='The index of key column to use for CSV format.')
    parser.add_argument('--delimiter', type=str, required=False, default=lib.DEFAULT_CSV_DELIMITER,
                        help='The delimiter to use for CSV format.')
    parser.add_argument('--field', required=False, default=lib.DEFAULT_JSON_FIELD,
                        help='The name of key field to use for JSON format.')
    parser.add_argument('--chunk-size', required=False, default=lib.DEFAULT_CHUNK_SIZE,
                        help='The number of lines to pack in a single gzip chunk.')
    parser.set_defaults(function=_repack)


def _repack(args):
    if args.input_file:
        fin = smart_open.open(args.input_file, 'rb', ignore_ext=True)
    else:
        fin = _BINARY_STDIN

    if not args.index_file and args.output_file:
        args.index_file = _strip_extension(args.output_file) + _GZIPI_EXTENSION

    if isinstance(args.input_file, str) and not _exists(args.input_file):
        _LOGGER.error("Input file does not exist: %s", args.input_file)
        _LOGGER.error("Aborting.")
        sys.exit(1)

    if not args.index_file:
        _LOGGER.error(
            "Can't determine path for index file. "
            "Please set it manually via --index-file parameter."
        )
        sys.exit(1)

    if P.exists(args.index_file):
        response = input(
            "Output index path already exists: %s."
            " Do you want to overwrite it? y/n\n" % args.index_file
        )
        if response.lower() not in _CONSENT_STRINGS:
            _LOGGER.error("Aborting.")
            sys.exit(1)

    if P.exists(args.output_file):
        response = input(
            "Output path already exists: %s."
            " Do you want to overwrite it? y/n\n" % args.output_file
        )
        if response.lower() not in _CONSENT_STRINGS:
            _LOGGER.error("Aborting.")
            sys.exit(1)

    index_fout = smart_open.open(args.index_file, 'wb')

    if args.output_file:
        fout = smart_open.open(args.output_file, 'wb', ignore_ext=True)
    else:
        fout = _BINARY_STDOUT

    if args.format == 'csv':
        lib.repack_csv_file(
            fin=fin, fout=fout,
            index_fout=index_fout, chunk_size=args.chunk_size,
            column=args.column, delimiter=args.delimiter
        )
    else:
        lib.repack_json_file(
            fin=fin, fout=fout,
            index_fout=index_fout, chunk_size=args.chunk_size,
            field=args.field,
        )
    fout.close()
    lib.sort_file(args.index_file)


def _create_parser():
    parser = argparse.ArgumentParser(description='gzipi command-line interface',
                                     usage=_CLI_DESCRIPTION)
    parser.add_argument(
        '-l', '--loglevel', default=logging.ERROR,
        help='Set the minimum level for log messages'
    )

    subparsers = parser.add_subparsers(help='sub-command --help')

    functions = [v for (k, v) in globals().items() if k.endswith('_subparser')]
    for func in sorted(functions, key=lambda f: f.__name__):
        func(subparsers)

    return parser


def main():
    parser = _create_parser()
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('smart_open').setLevel(logging.ERROR)

    logging.debug('args: %r', args)

    try:
        function = args.function
    except AttributeError:
        parser.error('try --help')
    else:
        function(args)


if __name__ == '__main__':
    main()
