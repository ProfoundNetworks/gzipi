=====
gzipi
=====

Tools for indexing gzip files to support random-like access.

Installing
~~~~~~~~~~

To install library from the source code, run the following coomand::

    $ python setup.py install

To install from pypi, run::

    $ pip install gzipi


Testing
~~~~~~~
::

    $ make test
    $ make lint

Repacking existing archives
===========================

If your archive was not converted before, you need to repack it::


    $ gzipi repack -f profiles.json.gz -i new_index.json.gz -o new_profiles.json.gz --format json --field domain


This command produces the repacked archive and the index file.


Retrieving data
================

To quickly retrieve data, you need a repacked archive and the index file.


Retrieving all data for specified domains from the stdin::

    $ cat domains_to_retrieve.txt | gzipi retrieve -f profiles.json.gz -i index.gzi--format json > data.json --field domain

Retrieving a single record name::

    $ gzipi search --input-file profiles.json.gz --index-file index.gzi --key google.com

Using local and S3 paths::

    $ gzipi retrieve -k domains.txt -f s3://logs/2019.json.gz -i index.json.gz --format json -o data.json --field domain


Indexing a file
===============

If you gzip archive is already chunked, you can index it without repacking.


Indexing a file from stdin::

    $ cat profiles.json.gz | gzipi index --format json --field id > index.json.gz

Indexing a local file::

    $ gzipi profiles.json.bz -i profiles.json.gz -o index.json.gz --format csv --column 0 --delimiter ','

Help
====

To get more information, run the following command::

    $ gzipi --help
