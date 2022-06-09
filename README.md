# datasette-socrata

[![PyPI](https://img.shields.io/pypi/v/datasette-socrata.svg)](https://pypi.org/project/datasette-socrata/)
[![Changelog](https://img.shields.io/github/v/release/simonw/datasette-socrata?include_prereleases&label=changelog)](https://github.com/simonw/datasette-socrata/releases)
[![Tests](https://github.com/simonw/datasette-socrata/workflows/Test/badge.svg)](https://github.com/simonw/datasette-socrata/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-socrata/blob/main/LICENSE)

Import data from Socrata into Datasette

## Installation

Install this plugin in the same environment as Datasette.

    datasette install datasette-socrata

## Usage

Once installed, an interface for importing data from Socrata will become available at this URL:

    /-/import-socrata

Users will be able to paste in a URL to a dataset on Socrata in order to initialize an import.

The `import-socrata` permission governs access. By default the `root` actor (accessible using `datasette --root` to start Datasette) is granted that permission.

You can use permission plugins such as [datasette-permissions-sql](https://github.com/simonw/datasette-permissions-sql) to grant additional access to other users.

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:

    cd datasette-socrata
    python3 -m venv venv
    source venv/bin/activate

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
