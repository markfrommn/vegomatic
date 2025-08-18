#!/usr/bin/env python3
"""
Simple example demonstrating the DataFetch module.
"""

from argparse import ArgumentParser
from typing import List, Dict, Union
import pprint

import dumper
from graphql import GraphQLSchema
from vegomatic.datafile import data_to_json_file

from vegomatic.datafetch import DataFetch
from vegomatic.datafile.fileparse import data_from_json_file

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
    parser.add_argument('--dburl', type=str, help='URL to Database')
    parser.add_argument('--table', type=str, help='Name of Table')
    parser.add_argument('--query', type=str, help='Query to run post create (Default SELECT * from <tablename>')
    parser.add_argument('--insertmethod', type=str, default='bulk', help='Insert method (bulk, insert, uori)')

    args = parser.parse_args()

    if args.datafile:
        data = data_from_json_file(args.datafile)
        unique_fields = None
        notnull_fields = None
    else:
        data = sample_data
        unique_fields = sample_unique_fields
        notnull_fields = sample_notnull_fields

    if args.dburl:
        test_db_url = args.dburl

    if args.table:
        test_table_name = args.table

    if args.query:
        test_query = args.query

    datafetch = example_datafetch_table(test_db_url, test_table_name, data, args.insertmethod, unique_fields, notnull_fields)

    if test_query is None:
        thetable = datafetch.get_table(test_table_name)
        retdata = datafetch.db().select(thetable.ALL)
    else:
        retdata = datafetch.db.executesql(test_query, as_dict=True)
    for row in retdata:
        pprint.pprint(row)

    print("\n=== All examples completed successfully! ===")