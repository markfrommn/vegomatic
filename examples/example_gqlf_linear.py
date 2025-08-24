#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module Linear module.
"""

import asyncio
import pprint

import dumper
from graphql import GraphQLSchema

from vegomatic.gqlf_linear import GqlFetchLinear

def pretty_print(clas, indent=0):
    print(' ' * indent +  type(clas).__name__ +  ':')
    indent += 4
    for k,v in clas.__dict__.items():
        if '__dict__' in dir(v):
            pretty_print(v,indent)
        else:
            print(' ' * indent +  k + ': ' + str(v))

def example_sync_fetch() -> GraphQLSchema:
    """Example of synchronous data fetching."""
    print("=== Synchronous Fetch Example ===")

    # Create a GqlFetch instance
    client = GqlFetchLinear(
        use_async=False
    )
    client.connect()

    # Define a GraphQL query
    query = """
        query {
            organization(login: "markfrommn") {
                repositories(first: 100) { # 'first' limits the number of results per page
                    totalCount
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        name
                        url
                        isPrivate
                        description
                        # Add other repository fields you need
                    }
                }
            }
        }
    """
    dsl_schema = None
    try:
        # Fetch data
        result = client.fetch_data(query)
        print("Repositories:")
        for repositories in result.get('repositories', []):
            print(f"  - {repo['name']} ({repo['url']})")
        dsl_schema = client.dsl_schema._schema

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
    return dsl_schema


async def example_async_fetch() -> GraphQLSchema:
    """Example of asynchronous data fetching."""
    print("\n=== Asynchronous Fetch Example ===")

    # Create an async GqlFetch instance
    async with GqlFetchLinear(
        use_async=True
    ) as client:
        client.connect()
        # Define a GraphQL query
        query = """
        query {
            organization(login: "markfrommn") {
                repositories(first: 100) { # 'first' limits the number of results per page
                    totalCount
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        name
                        url
                        isPrivate
                        description
                        # Add other repository fields you need
                    }
                }
            }
        }
        """
        dsl_schema = None
        try:
            # Fetch data asynchronously
            result = await client.fetch_data_async(query)
            print("Repositories (first 5):")
            for repository in result.get('repositories', [])[:5]:
                name = repository.get('name', 'N/A')
                url = repository.get('url', 'N/A')
                print(
                    f"  - {name} ({url})")
            # dsl_schema = client.dsl_schema._schema

        except Exception as e:
            print(f"Error: {e}")
        return dsl_schema


def example_data_extraction():
    """Example of data extraction from nested responses."""
    print("\n=== Data Extraction Example ===")

    client = GqlFetchLinear(
        use_async=False
    )
    client.connect()

    query = """
    query {
        owner(login: "{owner}") {
            repositories(first: 10) { # 'first' limits the number of results per page
            
                pullRequests(first: [], orderBy: { field: CREATED_AT, direction: DESC }) {
                    nodes {
                        number
                        title
                        state
                        author {
                            login
                        }
                        createdAt
                        mergedAt
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    }
    """

    try:
        # Extract only the countries array
        countries = client.fetch_data(
            query,
            extract_path="repositories"
        )

        print("Repositories with PRs:")
        for repository in repositories[:5]:  # Show first 5
            name = repository.get('name', 'Unknown')
            url = repository.get('url', 'Unknown')
            print(
                f"  - {name} ({url})")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


def example_page_info():
    """Example of PageInfo usage."""
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
    dumper.max_depth = 20
    dumper.instance_dump = 'all'
    pprint.pprint(dsl_schema.query_type.fields)
    #dumper.dump(dsl_schema)

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
