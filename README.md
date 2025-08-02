# Veg-o-Matic

Veg-o-Matic is a multi-module library of various Python Utilities that I have evolved over the years.  It originally
started as PyDataXlate to provide a data driven field mapping of data sources that I used for various website migrations.

Basic functions include:

- File operations
- Simple database layer
- Data translation from PyDataXlate.
- GraphQL client with plugable extensions.

PyDataXlate used setuptools.  For Veg-o-Matic I'm taking my first foray into Poetry.

Veg-o-Matic lives here at [https://github.com/markfrommn/vegomatic](https://github.com/markfrommn/vegomatic).

## Available Modules

### datadb
A simple library of database access.

- Simple database layer that abstracts out the DB layer to provide simple support for the most popular databases.
  - MySQL
  - Postgres
  - sqlite3

### datafetch
A wrapper around datadb to fetch data from databases into various convenient program friendly forms including:
- A list of dictionaries.

### datafile
A simple set of routines to get sstructured data to/from text files.

### datamap
A datamapping module that maps datasets using data driven field mapping including simple field names, custom translation, etc.

### gqlfetch
A mid-level library to fetch object from a GraphQL API using a derivable class with API specific templates for queries and paginations.

### gqlf-modules
Example modules for gqlfetch.  These will likely move into their own repository at some point.
- GitHub - Access users, repos, PRs and commits for a Github organization / owner.

## Project Structure

```
python-snippets/
├── README.md
├── README_datadb.md
├── README_datafetch.md
├── README_datafile.md
├── README_datamap.md
├── README_gqlfetch.md
├── README_gqlf-modules.md
├── requirements.txt
├── setup.py
├── examples/
│   ├── example_datadb.py
│   ├── example_datafetch.py
│   ├── example_datafile.py
│   ├── example_datamap.py
│   ├── example_gqlfetch.py
│   └── example_gqlf-github.py
├── src/vegomatic/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   ├── datafetch/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   ├── datafile/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   ├── datamap/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   ├── gqlfetch/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   ├── gqlf-github/
│   │   ├── __init__.py
│   │   ├── foo.py
│   │   └── boo.py
│   └── ...
├── tests/
│   ├── test_datadb.py
│   ├── test_datafetch.py
│   ├── test_datafile.py
│   ├── test_datamap.py
│   ├── test_gqlfetch.py
│   └── test_gqlf-github.py
├── ...
```

## How to Install / Use
Pythonistas can ignore and not be offended by the obvious tutorial on pyenv.

### pyenv

It is strongly recommended to use pyenv or similar in general.  Assuming you have [pyenv](https://github.com/pyenv/pyenv), then to start then something like:
```bash
# Because of course you can never remember what is available.
pyenv versions 

 # Set to what you want venv to setup with
pyenv local 3.13.1

# Because pip is always out of date and will complain endlessly, but this doesn't actually fix the venv...
pip install --upgrade pip 

# Initialize the venv (or reset to current version)
# And pass --upgrade-deps because venv will downgrade your pip because the ensurepip package has a bundled pip bound to the python version
python3 -m venv --upgrade-deps .venv

# activate the venv
source .venv/bin/activate

# Verify you are happily getting python from the venv
which python

<whereiam>/vegomatic/.venv/bin/python

# ... do lots of stuff - when done, turn off the venv...
deactivate

# Trust but verify...
which python
Somewhere in <yourhomedir> , /usr, /opt, etc

...unless you are a heathen and do *pyenv activate system*.
```
Now you are ready to get python ready to use the actual code.
```bash
# Install dependencies
pip install -r requirements.txt

# Install the packagse in development mode
pip install -e .
```

## How to Use

Import modules in your Python code:

```python
# Blah
```

## Testing

Run the test suites to verify everything works:

```bash
python tests/test_datadb.py
python tests/test_datafetch.py
python tests/test_datafile.py
python tests/test_datamap.py
python tests/test_gqlfetch.py
python tests/test_gqlf-github.py
```

## Examples

Run the example scripts to see the modules in action:

```bash
python examples/example_datadb.py
python examples/example_datafetch.py
python examples/example_datafile.py
python examples/example_datamap.py
python examples/example_gqlfetch.py
python examples/example_gqlf-github.py
```

## Adding Modules

Add new modules as subdirectories under `src/vegomatic/`, each with an `__init__.py` file.

## History Notes
This was a a Python library for mapping data meant for use in data export/import cases including data driven field mapping and translation called PyDataXlate.  It was used for vBulletin -> WordPress -> Xenforo migration over the years for www.beechaeroclub.org (and the Xenforo subsite core.beechaereoclub.org).

Please note that any existing invention disclosures and Copyright that reference PyDataXlate library apply to Veg-o-Matic and follow it here as Veg-o-Matic is a direct derivation of that work.  As the copyright holder I revoke any previous license for this derived code other than the licensing given in this repository.

