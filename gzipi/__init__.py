#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2019
#
"""Top-level package for gzipi."""
import os.path as P

from .lib import index_csv_file, index_json_file, retrieve, search
from .lib import repack_json_file, repack_csv_file


with open(P.join(P.dirname(__file__), 'VERSION')) as fin:
    __version__ = fin.readline().rstrip()
