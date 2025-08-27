#!/usr/bin/env python3
"""
Simple example demonstrating the DataFetch module.
"""

from argparse import ArgumentParser
from typing import List, Mapping, Union
import pprint

import dumper
import json
from graphql import GraphQLSchema
from vegomatic.datafile import FileSet, dicts_from_files
import pandas as pd

from vegomatic.datafetch import DataFetch
from vegomatic.datafile.fileparse import data_from_json_file
from vegomatic.datamap import flatten_to_dict

sample_data = [
    {
        'name': 'John Doe',
        'age': 30,
        'email': 'john.doe@example.com',
        'weight': 180.5,
        'birthdate': '1990-03-15 21:12:00',
        'is_male': True
    },
    {
        'name': 'Jane Doe',
        'age': 25,
        'email': 'jane.doe@example.com',
        'weight': 150.1,
        'birthdate': '1995-07-15T18:30:00Z',
        'is_male': "N"
    },
    {
        'name': 'Baby John',
        'age': 1,
        'weight': 7.9,
        'birthdate': '2024-07-15T18:30:00Z',
        'is_male': "N"
    }
]
sample_unique_fields = ['name']
sample_notnull_fields = ['name', 'birthdate', 'is_male']

test_db_url = 'sqlite://test.db'
test_table_name = 'test'
test_query = None

def pretty_print(clas, indent=0):
    print(' ' * indent +  type(clas).__name__ +  ':')
    indent += 4
    for k,v in clas.__dict__.items():
        if '__dict__' in dir(v):
            pretty_print(v,indent)
        else:
            print(' ' * indent +  k + ': ' + str(v))

def example_datafetch_table(dburl: str, tablename: str, data: list[dict],  insert_method: str = 'bulk', unique_fields: list[str] = None, notnull_fields: list[str] = None) -> DataFetch | None:
    """Example of Data to an pydal table."""
    print("=== Data to pydal Table Example ===")

    # Create a DataFetch instance
    datafetch = DataFetch()
    if not datafetch.create(dburl):
        raise RuntimeError("Failed to create database connection.")

    schema = datafetch.fields_from_dicts(data, unique_fields, notnull_fields)
    if schema is None:
        raise RuntimeError("Failed to create schema.")

    print("=== Schema ===")
    # TO DO: pretty print the schema with something that works
    #pprint.pprint(schema)

    # Create a table
    datafetch.create_table(tablename, schema)

    print("=== Tables ===")

    # Insert data
    if insert_method == 'bulk':
        datafetch.db[tablename].bulk_insert(data)
    elif insert_method == 'insert':
        for row in data:
            datafetch.db[tablename].insert(**row)
    elif insert_method == 'uori':
        for row in data:
            datafetch.db[tablename].update_or_insert(_key={'name': row['name']}, **row)
    datafetch.db.commit()
    return datafetch

if __name__ == "__main__":
    parser = ArgumentParser(description='Data Fetch')
    parser.add_argument('--datafile', type=str, help='Path to Datafile')
    parser.add_argument('--datafileset', type=str, help='Glob to Datafile')
    parser.add_argument('--datafilekey', type=str, default='id', help='Datafile ID Key')
    parser.add_argument('--datakeys', type=str, default='id,identifier', help='Datafile Unique Keys')
    parser.add_argument('--notnullkeys', type=str, default=None, help='Datafile Not Null Keys')
    parser.add_argument('--showdata', action='store_true', help='Show the data after load/fixup')
    parser.add_argument('--showfields', action='store_true', help='Show the fields')
    parser.add_argument('--createdb', action='store_true', help='Actually create the database')
    parser.add_argument('--dburl', type=str, default=None, help='URL to Database')
    parser.add_argument('--table', type=str, help='Name of Table')
    parser.add_argument('--query', type=str, help='Query to run post create (Default SELECT * from <tablename>')
    parser.add_argument('--insertmethod', type=str, default='bulk', help='Insert method (bulk, insert, uori)')

    args = parser.parse_args()

    if args.datafile and not args.datafileset:
        in_data = data_from_json_file(args.datafile)
        unique_fields = None
        notnull_fields = None
    elif args.datafileset:
        fileset = FileSet()
        fileset.glob(args.datafile, args.datafileset, True)
        print(f"Found {len(fileset.filepaths)} files from {args.datafileset}")
        if args.datakeys is not None:
            unique_fields = args.datakeys.split(',')
        else:
            unique_fields = None
        if args.notnullkeys is not None:
            notnull_fields = args.notnullkeys.split(',')
        else:
            notnull_fields = None
        nokeys = []
        ( in_data, nokeys) = dicts_from_files(fileset, args.datafilekey, "json")
        print(f"Found {len(in_data)} items from {args.datafileset}")
        print(f"Found {len(nokeys)} items without keys from {args.datafileset}")
    else:
        in_data = sample_data
        unique_fields = sample_unique_fields
        notnull_fields = sample_notnull_fields

    if args.dburl:
        test_db_url = args.dburl

    if args.table:
        test_table_name = args.table

    if args.query:
        test_query = args.query

    # Listify our arguments from single instance or dict
    if isinstance(in_data, Mapping):
        in_data = in_data.values()
    elif not isinstance(in_data, list):
        in_data = [in_data]
    #print(json.dumps(in_data, indent=4))
    data = []
    #for item in in_data:
    #    print(f"Item: {item}")

    for item in in_data:
        item = DataFetch.fix_item(item, test_table_name)
        data.append(item)

    if args.showdata:
        print(json.dumps(data, indent=4))

    if args.showfields:
        # fields_from_dicts needs a list of dicts, not a dict
        fields = DataFetch.fields_from_dicts(data, unique_fields, notnull_fields)
        field_dicts = []
        for field in fields:
            field_dicts.append(field.__dict__)
        columns = [ 'name', 'type', 'length', 'default', 'required', 'unique', 'notnull', 'comment' ]
        df = pd.DataFrame(field_dicts)
        df = df.loc[:, df.columns.isin(columns)]
        print(df.to_string())

    if not args.createdb:
        # just testing previous stuff...
        exit(0)

    datafetch = example_datafetch_table(test_db_url, test_table_name, data, args.insertmethod, unique_fields, notnull_fields)

    if test_query is None:
        thetable = datafetch.get_table(test_table_name)
        retdata = datafetch.db().select(thetable.ALL)
    else:
        retdata = datafetch.db.executesql(test_query, as_dict=True)
    for row in retdata:
        pprint.pprint(row)

    print("\n=== All examples completed successfully! ===")