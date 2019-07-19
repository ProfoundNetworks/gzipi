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
import gzip
import logging
import sys
import os.path as P

from . import lib

_LOGGER = logging.getLogger(__name__)
_BINARY_STDIN, _BINARY_STDOUT = sys.stdin.buffer, sys.stdout.buffer


_CLI_DESCRIPTION = """gzipi  <command> [<args>]

Available commands:
    gzipi index     Scan a file to create a new index.
    gzipi retrieve  Use a previously created index to quickly access individual
                    lines in the compressed file.
    gzipi repack    Recompress a gzip file and create a new index for it.
"""


def _index_subparser(args):
    desc = 'Scan a file to create a new index.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-i', '--input-file', required=False,
        help='The path to the file to index. If path is not specified, reads from stdin.'
    )
    parser.add_argument(
        '-o', '--index-file', required=False,
        help='The path to save gzipped output to. If path is not specified, outputs to stdout.'
    )
    parser.add_argument('--format', required=True, choices=lib.FILE_FORMATS,
                        help='The format of the input file.')
    parser.add_argument('--column', type=int, required=False, default=lib.DEFAULT_CSV_COLUMN,
                        help='The index of key column to use for CSV format.')
    parser.add_argument('--delimiter', type=str, required=False, default=lib.DEFAULT_CSV_DELIMITER,
                        help='The delimiter to use for CSV format.')
    parser.add_argument('--field', required=False, default=lib.DEFAULT_JSON_FIELD,
                        help='The name of key field to use for JSON format.')
    parser.add_argument('--index-lines', action='store_true',
                        help='If set, indexes lines inside gzip chunks as well.')

    args = parser.parse_args(args)
    fin = open(args.input_file, 'rb') if args.input_file else _BINARY_STDIN
    if args.index_file and P.exists(args.index_file):
        raise ValueError("Output index path already exists: %s" % args.index_file)
    fout = gzip.open(args.index_file, 'wb') if args.index_file else _BINARY_STDOUT

    if args.format == 'csv':
        lib.index_csv_file(csv_file=fin, output_file=fout, column=args.column,
                           delimiter=args.delimiter, index_lines=args.index_lines)
    else:
        lib.index_json_file(json_file=fin, output_file=fout, field=args.field,
                            index_lines=args.index_lines)


def _retrieve_subparser(args):
    desc = 'Scan a file for a provided list of keys given an index file.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-k', '--keys', required=False,
        help='The path to the key strings (e.g. domains) to scan the input file for. '
             'If path is not specified, reads from stdin.'
    )
    parser.add_argument('-f', '--input-file', required=True,
                        help='The path to input file to scan. May be a local path or an S3 path.')
    parser.add_argument('-i', '--index-file', required=True,
                        help='The local path to read index data from.')
    parser.add_argument('-o', '--output-file', required=False,
                        help='The path to save gzipped output to. By default, outputs to stdout.')
    parser.add_argument('--format', required=True, choices=lib.FILE_FORMATS,
                        help='The format of the input file.')
    parser.add_argument('--column', type=int, required=False, default=lib.DEFAULT_CSV_COLUMN,
                        help='The index of key column to use for CSV format.')
    parser.add_argument('--delimiter', type=str, required=False, default=lib.DEFAULT_CSV_DELIMITER,
                        help='The delimiter to use for CSV format.')
    parser.add_argument('--field', required=False, default=lib.DEFAULT_JSON_FIELD,
                        help='The name of key field to use for JSON format.')

    args = parser.parse_args(args)
    if args.keys:
        if args.keys.endswith(".gz"):
            keys_fin = gzip.open(args.keys, 'rb')
        else:
            keys_fin = open(args.keys, 'rb')
    else:
        keys_fin = _BINARY_STDIN
    fout = gzip.open(args.output_file, 'wb') if args.output_file else _BINARY_STDOUT

    if args.format == 'csv':
        lib.retrieve_from_csv(
            keys_fin=keys_fin, csv_path=args.input_file,
            index_path=args.index_file, output_stream=fout,
            column=args.column, delimiter=args.delimiter
        )
    else:
        lib.retrieve_from_json(
            keys_fin=keys_fin, json_path=args.input_file,
            index_path=args.index_file, output_stream=fout,
            field=args.field
        )


def _repack_subparser(args):
    desc = 'Repack a gzipped file into a chunked gzipped file and an index file.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-f', '--input-file', required=False,
        help='The path to the input file to repack. If file is not specified, reads from stdin.'
    )
    parser.add_argument('-o', '--output-file', required=False,
                        help='The path to save recompressed file to.')
    parser.add_argument('-i', '--index-file', required=True,
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
    args = parser.parse_args(args)

    fin = open(args.input_file, 'rb') if args.input_file else _BINARY_STDIN
    index_fout = gzip.open(args.index_file, 'wb')
    fout = open(args.output_file, 'wb') if args.output_file else _BINARY_STDOUT

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


def _create_main_parser():
    parser = argparse.ArgumentParser(description='gzippi command-line interface',
                                     usage=_CLI_DESCRIPTION)
    parser.add_argument('command', help='Subcommand to run')
    args = parser.parse_args(sys.argv[1:2])
    subparser = '_%s_subparser' % args.command
    if subparser not in globals():
        print('Unrecognized command: %r' % args.command)
        parser.print_help()
        return
    return globals()[subparser](sys.argv[2:])


def main():
    _create_main_parser()


if __name__ == '__main__':
    main()
