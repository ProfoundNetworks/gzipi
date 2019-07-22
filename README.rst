=====
gzipi
=====

Tools for indexing gzip files to support random-like access.

Installing
~~~~~~~~~~

To install library, run the following coomand::

    $ python setup.py install

Testing
~~~~~~~
::

    $ make test
    $ make lint


Indexing a file
===============

Indexing a file from stdin::

    $ cat profiles.json.gz | gzipi index --format json --field id > index.json.gz

Indexing a local file::

    $ cat profiles.json.bz -i profiles.json.gz -o index.json.gz --format csv --column 0 --delimiter ','


Retrieving data
================

Retrieving all data for specific domains::

    $ zcat domains.json.gz | gzipi retrieve -f profiles.json.gz -i index.json.gz --format json > data.json --field domain


Using local and S3 paths::

    $ gzipi retrieve -k domains.json.gz -f s3://domain-data/reports/Q1.json.gz -i index.json.gz --format json -o data.json --field domain

Repacking existing archives
===========================
::

    $ gzipi repack -f profiles.json.gz -i new_index.json.gz -o new_profiles.json.gz --format json --field domain

Help
====

To get more information, run the following command::

    $ gzipi --help
