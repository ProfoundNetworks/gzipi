#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import io
import os.path as P
from setuptools import setup, find_packages


def read(fname):
    return io.open(P.join(P.dirname(__file__), fname), encoding='utf-8').read()


version = read('gzipi/VERSION').split('\n')[0]
requirements = ['smart-open', 'plumbum']
setup_requirements = []
test_requirements = ['pytest', 'flake8', ]

setup(
    author="Profound Networks",
    author_email='mpenkov@profound.net',
    url='https://github.com/ProfoundNetworks/gzipi',
    download_url='https://pypi.org/project/gzipi',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    install_requires=requirements,
    include_package_data=True,
    package_data={
        'gzipi': [
            'VERSION',
        ],
    },
    name='gzipi',
    packages=find_packages(include=['gzipi']),
    setup_requires=setup_requirements,
    test_suite='tests',
    description="Tools for indexing gzip files to support random-like access.",
    long_description=read('README.rst'),
    tests_require=test_requirements,
    version=version,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'gzipi=gzipi.cli:main',
        ]
    },
)
