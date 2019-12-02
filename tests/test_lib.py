#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
import gzip
import io
import unittest

import gzipi.lib


def _gzip_data(data):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='w') as fout:
        fout.write(data)
    return buf.getvalue()


class IterateArchivestest(unittest.TestCase):
    def test_iterates_archives(self):
        chunks = [
            _gzip_data(b'chunk number 1' * 2),
            _gzip_data(b'chunk  #2' * 2),
            _gzip_data(b'chunk num 3' * 2),
        ]
        buf = io.BytesIO()
        for chunk in chunks:
            buf.write(chunk)
        buf.flush()
        buf.seek(0)
        expected = [(chunks[0], 0, 37), (chunks[1], 37, 68), (chunks[2], 68, 101)]
        actual = [
            (chunk[0].getvalue(), chunk[1], chunk[2])
            for chunk in gzipi.lib._iterate_archives(buf, buffer_size=15)
        ]
        self.assertEqual(expected, actual)


class StartOfLineTest(unittest.TestCase):
    def setUp(self):
        self.fin = io.BytesIO(b'one\ntwo\nthree\nfour\nfive\nsix\nseven')

    def test_start(self):
        self.fin.seek(2)
        gzipi.lib._start_of_line(self.fin)

        expected = b'one\n'
        actual = self.fin.readline()
        self.assertEqual(expected, actual)

    def test_middle(self):
        self.fin.seek(10)
        gzipi.lib._start_of_line(self.fin)

        expected = b'three\n'
        actual = self.fin.readline()
        self.assertEqual(expected, actual)

    def test_end(self):
        self.fin.read()
        gzipi.lib._start_of_line(self.fin)

        expected = b'seven'
        actual = self.fin.readline()
        self.assertEqual(expected, actual)


class BinarySearchTest(unittest.TestCase):
    def setUp(self):
        self.fin = io.BytesIO(
            b"a|1\n"
            b"b|2\n"
            b"c|3\n"
        )
        self.fsize = len(self.fin.getvalue())

    def test_start(self):
        actual = gzipi.lib._binary_search(b'a', self.fin, self.fsize)
        self.assertEqual([b'1'], actual)

    def test_middle(self):
        actual = gzipi.lib._binary_search(b'b', self.fin, self.fsize)
        self.assertEqual([b'2'], actual)

    def test_end(self):
        actual = gzipi.lib._binary_search(b'c', self.fin, self.fsize)
        self.assertEqual([b'3'], actual)

    def test_missing(self):
        with self.assertRaises(KeyError):
            gzipi.lib._binary_search(b'd', self.fin, self.fsize)

    def test_exits_from_the_last_line_in_the_middle(self):
        fin = io.BytesIO(
            b"key1|0|100|300|500\n"
            b"key2|0|100|400|1500\n"
            b"key3|0|200|400|1500\n"
            b"key4|0|300|500|22500\n"
            b"key5|0|400|3600|33500n\n"
        )
        with self.assertRaises(KeyError):
            gzipi.lib._binary_search(b'key33', fin, self.fsize)


class BufferChunkTest(unittest.TestCase):
    def setUp(self):
        self.lines = (
            b"key1|0|100|300|500\n",
            b"key2|0|100|400|1500\n",
            b"key3|0|200|400|1500\n",
            b"key4|0|200|400|1500\n",
        )

    def test_buffers_chunk(self):
        fin = io.BytesIO(b"".join(self.lines))
        expected_scope = (0, 40, 3)

        fin, start, end, pivot = gzipi.lib._buffer_chunk(fin, 25, 50, 22, b'\n')
        self.assertEqual(fin.read(100), b"".join(self.lines[1:-1]))
        self.assertEqual(expected_scope, (start, end, pivot))

    def test_buffers_ideal_chunk(self):
        fin = io.BytesIO(b"".join(self.lines))
        expected_scope = (0, 20, 19)
        fin, start, end, pivot = gzipi.lib._buffer_chunk(fin, 19, 39, 38, b'\n')
        self.assertEqual(fin.read(100), self.lines[1])
        self.assertEqual(expected_scope, (start, end, pivot))


class RepackEmptyTest(unittest.TestCase):
    def setUp(self):
        self.fin = io.BytesIO()
        self.fout = io.BytesIO()
        self.index_fout = io.BytesIO()
        self.gzip_header = gzip.compress(b'')

    def test_csv_contains_gzip_header(self):
        gzipi.lib.repack_csv_file(self.fin, self.fout, self.index_fout, 1000)
        self.assertEqual(self.fout.getvalue(), self.gzip_header)

    def test_json_contains_gzip_header(self):
        gzipi.lib.repack_json_file(self.fin, self.fout, self.index_fout, 1000)
        self.assertEqual(self.fout.getvalue(), self.gzip_header)
