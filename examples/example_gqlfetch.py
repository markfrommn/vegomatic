#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import asyncio
import pprint

import dumper
from graphql import GraphQLSchema

from vegomatic.gqlfetch import GqlFetch

def pretty_print(clas, indent=0):
    """
    Print a class object in a formatted, indented structure.

    Args:
        clas: The class object to print
        indent: Current indentation level (default: 0)
    """
    print(' ' * indent + type(clas).__name__ + ':')
    indent += 4
    for k, v in clas.__dict__.items():
        if '__dict__' in dir(v):
            pretty_print(v, indent)
        else:
            print(' ' * indent + k + ': ' + str(v))

def example_sync_fetch() -> GraphQLSchema:
    """
    Example of synchronous data fetching.

    Returns:
        GraphQLSchema: The DSL schema from the GraphQL endpoint
    """
    print("=== Synchronous Fetch Example ===")

    # Create a GqlFetch instance
    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    client.connect()

    # Define a GraphQL query
    query = """
    query getContinents {
      continents {
        code
        name
      }
    }
    """
    dsl_schema = None
    try:
        # Fetch data
        result = client.fetch_data(query)
        print("Continents:")
        for continent in result.get('continents', []):
            print(f"  - {continent['name']} ({continent['code']})")
        dsl_schema = client.dsl_schema._schema

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
    return dsl_schema


async def example_async_fetch() -> GraphQLSchema:
    """
    Example of asynchronous data fetching.

    Returns:
        GraphQLSchema: The DSL schema from the GraphQL endpoint
    """
    print("\n=== Asynchronous Fetch Example ===")

    # Create an async GqlFetch instance
    async with GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=True
    ) as client:
        client.connect()
        # Define a GraphQL query
        query = """
        query getCountries {
          countries {
            code
            name
            capital
          }
        }
        """
        dsl_schema = None
        try:
            # Fetch data asynchronously
            result = await client.fetch_data_async(query)
            print("Countries (first 5):")
            for country in result.get('countries', [])[:5]:
                capital = country.get('capital', 'N/A')
                print(
                    f"  - {country['name']} ({country['code']}) - Capital: {capital}")
            # dsl_schema = client.dsl_schema._schema

        except Exception as e:
            print(f"Error: {e}")
        return dsl_schema


def example_data_extraction():
    """
    Example of data extraction from nested responses.

    Demonstrates how to extract specific data from nested GraphQL responses
    using the extract_path parameter.
    """
    print("\n=== Data Extraction Example ===")

    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    client.connect()

    query = """
    query getCountryData {
      countries {
        code
        name
        continent {
          name
        }
      }
    }
    """

    try:
        # Extract only the countries array
        countries = client.fetch_data(
            query,
            extract_path="countries"
        )

        print("Countries with continents:")
        for country in countries[:5]:  # Show first 5
            continent = country.get('continent', {}).get('name', 'Unknown')
            print(
                f"  - {country['name']} ({country['code']}) - Continent: {continent}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


def example_page_info():
    """
    Example of PageInfo usage.

    Demonstrates how to create and use PageInfo objects for pagination.
    """
    print("\n=== PageInfo Example ===")

    from vegomatic.gqlfetch import PageInfo

    # Create a PageInfo instance
    page_info = PageInfo(
        has_next_page=True,
        has_previous_page=False,
        start_cursor="start123",
        end_cursor="end456"
    )

    print(f"PageInfo: {page_info}")
    print(f"Has next page: {page_info.has_next_page}")
    print(f"Has previous page: {page_info.has_previous_page}")
    print(f"Start cursor: {page_info.start_cursor}")
    print(f"End cursor: {page_info.end_cursor}")


def example_dsl_schema(dsl_schema):
    """
    Example of DSL schema exploration.

    Args:
        dsl_schema: The GraphQL schema to explore and print
    """
    dumper.max_depth = 20
    dumper.instance_dump = 'all'
    pprint.pprint(dsl_schema.query_type.fields)
    # dumper.dump(dsl_schema)

if __name__ == "__main__":
    # Run sync example
    dsl_schema_sync = example_sync_fetch()

    # Run async example
    dsl_schema_async = asyncio.run(example_async_fetch())

    # Run data extraction example
    example_data_extraction()

    # Run PageInfo example
    example_page_info()

    # Print the DSL schema
    example_dsl_schema(dsl_schema_sync)

    print("\n=== All examples completed successfully! ===")
