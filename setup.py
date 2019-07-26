#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages


requirements = ['smart-open', 'plumbum']
setup_requirements = []
test_requirements = ['pytest', 'flake8', ]

setup(
    author="Profound Networks",
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
    name='gzipi',
    packages=find_packages(include=['gzipi']),
    setup_requires=setup_requirements,
    test_suite='tests',
    desription="Tools for indexing gzip files to support random-like access.",
    tests_require=test_requirements,
    version='0.1.0',
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'gzipi=gzipi.cli:main',
        ]
    },
)
