#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#

import gzip
import io
import os.path as P

import gzipi.lib


def test_indexes_json_file():
    curr_dir = P.dirname(P.abspath(__file__))
    json_file = open(P.join(curr_dir,  'data/sample.json.gz'), 'rb')
    output_file = io.BytesIO()
    gzipi.lib.index_json_file(json_file, output_file, field='id', min_chunk_size=1000)
    expected = gzip.open(P.join(curr_dir,  'data/index.json.gz'), 'rb').read()
    actual = output_file.getvalue()
    assert expected == actual


def test_retrieves_indexed_json():
    curr_dir = P.dirname(P.abspath(__file__))
    json_path = P.join(curr_dir,  'data/sample.json.gz')
    index_fin = gzip.open(P.join(curr_dir, 'data/index.json.gz'), 'rt')
    keys = io.BytesIO(
        b"95-926-1252\n00-720-2041"
        b"\n17-517-6091\n06-589-6091\n37-510-3515"
    )
    output_file = io.BytesIO()

    gzipi.lib.retrieve(keys, json_path, index_fin, output_file)
    actual = output_file.getvalue()
    expected = open(P.join(curr_dir,  'data/retrieve_expected.json'), 'rb').read()
    assert expected == actual


def test_indexes_csv_file():
    curr_dir = P.dirname(P.abspath(__file__))
    json_file = open(P.join(curr_dir,  'data/sample.csv.gz'), 'rb')
    output_file = io.BytesIO()
    gzipi.lib.index_csv_file(json_file, output_file, column=0, delimiter=',', min_chunk_size=1000)
    expected = gzip.open(P.join(curr_dir,  'data/index.csv.gz'), 'rb').read()
    actual = output_file.getvalue()
    assert expected == actual


def test_retrieves_indexed_csv():
    curr_dir = P.dirname(P.abspath(__file__))
    json_path = P.join(curr_dir,  'data/sample.csv.gz')
    index_fin = gzip.open(P.join(curr_dir, 'data/index.csv.gz'), 'rt')
    keys = io.BytesIO(
        b"56-053-2131\n09-530-5619"
        b"\n25-172-0048\n11-111-1148\n20-429-6275"
    )
    output_file = io.BytesIO()

    gzipi.lib.retrieve(keys, json_path, index_fin, output_file)
    actual = output_file.getvalue()
    expected = open(P.join(curr_dir,  'data/retrieve_expected.csv'), 'rb').read()
    assert expected == actual


if __name__ == '__main__':
    test_indexes_json_file()
    test_retrieves_indexed_json()
    test_indexes_csv_file()
    test_retrieves_indexed_csv()
