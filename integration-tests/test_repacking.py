#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#

import gzip
import io
import os.path as P

import gzipi.lib


def test_repacks_json_file():
    curr_dir = P.dirname(P.abspath(__file__))
    json_file = open(P.join(curr_dir, 'data/sample.json.gz'), 'rb')
    fout = io.BytesIO()
    index_fout = io.BytesIO()
    gzipi.lib.repack_json_file(
        fin=json_file,
        fout=fout,
        index_fout=index_fout,
        chunk_size=50,
        field='id'
    )
    expected_index = gzip.open(P.join(curr_dir, 'data/repacked_index.json.gz'), 'rb').read()
    actual_index = index_fout.getvalue()
    assert actual_index == expected_index
    assert fout.getvalue().count(gzipi.lib._GZIP_HEADER) == 20


def test_repacks_csv_file():
    curr_dir = P.dirname(P.abspath(__file__))
    csv_file = open(P.join(curr_dir, 'data/sample.csv.gz'), 'rb')
    fout = io.BytesIO()
    index_fout = io.BytesIO()
    gzipi.lib.repack_csv_file(
        fin=csv_file,
        fout=fout,
        index_fout=index_fout,
        chunk_size=50,
        column=0,
        delimiter=','
    )
    expected_index = gzip.open(P.join(curr_dir, 'data/repacked_index.csv.gz'), 'rb').read()
    actual_index = index_fout.getvalue()
    assert actual_index == expected_index
    assert fout.getvalue().count(gzipi.lib._GZIP_HEADER) == 21


if __name__ == '__main__':
    test_repacks_json_file()
    test_repacks_csv_file()
