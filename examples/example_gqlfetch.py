#!/usr/bin/env python3
"""
Example usage of the GqlFetch class for fetching data from GraphQL endpoints.
"""

import asyncio
from typing import Dict, Any
from vegomatic.gqlfetch import GqlFetch


def example_basic_fetch():
    """Example of basic data fetching."""
    print("=== Basic Fetch Example ===")
    
    # Create a GqlFetch instance
    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    
    # Define a GraphQL query
    query = """
    query getContinents {
      continents {
        code
        name
      }
    }
    """
    
    try:
        # Fetch data
        result = client.fetch_data(query)
        print("Continents:", result)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


def example_paginated_fetch():
    """Example of paginated data fetching."""
    print("\n=== Paginated Fetch Example ===")
    
    # Create a GqlFetch instance
    client = GqlFetch(
        endpoint="https://api.github.com/graphql",
        headers={
            "Authorization": "Bearer YOUR_GITHUB_TOKEN"  # Replace with actual token
        },
        use_async=False
    )
    
    # Define a GraphQL query with pagination
    query = """
    query getRepositories($after: String) {
      viewer {
        repositories(first: 10, after: $after) {
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            node {
              name
              description
              stargazerCount
            }
          }
        }
      }
    }
    """
    
    try:
        # Fetch paginated data
        for page_num, page_data in enumerate(client.fetch_paginated(query, max_pages=3)):
            print(f"Page {page_num + 1}:")
            for repo in page_data:
                node = repo.get('node', {})
                print(f"  - {node.get('name')}: {node.get('stargazerCount')} stars")
            print()
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


async def example_async_fetch():
    """Example of async data fetching."""
    print("\n=== Async Fetch Example ===")
    
    # Create an async GqlFetch instance
    async with GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=True
    ) as client:
        
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
        
        try:
            # Fetch data asynchronously
            result = await client.fetch_data_async(query)
            print("Countries:", result)
            
        except Exception as e:
            print(f"Error: {e}")


def example_dsl_usage():
    """Example of using DSL for query building."""
    print("\n=== DSL Usage Example ===")
    
    # Create a GqlFetch instance
    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    
    try:
        # Create DSL schema (this would typically be done with a real schema)
        from gql.dsl import DSLSchema, DSLQuery, DSLField
        
        # Note: In a real scenario, you would create the schema from the GraphQL schema
        # This is a simplified example
        schema = DSLSchema(client.client.schema)
        client.set_dsl_schema(schema)
        
        # Create a query using DSL
        # This is a conceptual example - actual implementation would depend on the schema
        query = client.create_dsl_query("getContinents")
        
        # Execute the query
        result = client.fetch_data(query)
        print("DSL Query Result:", result)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


def example_custom_extraction():
    """Example of custom data extraction paths."""
    print("\n=== Custom Extraction Example ===")
    
    # Create a GqlFetch instance
    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    
    # Define a GraphQL query
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
        # Fetch data with custom extraction path
        result = client.fetch_data(
            query,
            extract_path="countries",  # Extract only the countries array
            page_info_path="pageInfo",
            edges_path="edges",
            nodes_path="nodes"
        )
        print("Extracted Countries:", result)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


async def main():
    """Run all examples."""
    # Sync examples
    example_basic_fetch()
    example_paginated_fetch()
    example_dsl_usage()
    example_custom_extraction()
    
    # Async example
    await example_async_fetch()


if __name__ == "__main__":
    # Run sync examples
    example_basic_fetch()
    example_paginated_fetch()
    example_dsl_usage()
    example_custom_extraction()
    
    # Run async example
    asyncio.run(example_async_fetch())
