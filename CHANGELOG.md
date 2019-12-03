# Change log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased

### Modified

- Improve binary search by buffering small search scopes
- Add transport_params keyword parameter for smart_open calls

### Fixed

- Fix binary search does not terminate correctly on unknown keys
- Fix repacking an empty file produces malformed gzip file

# [0.1.1 ] - 2019-08-20

- First PyPI release.
