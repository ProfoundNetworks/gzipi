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
