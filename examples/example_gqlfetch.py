#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import asyncio
from vegomatic.gqlfetch import GqlFetch


def example_sync_fetch():
    """Example of synchronous data fetching."""
    print("=== Synchronous Fetch Example ===")
    
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
        print("Continents:")
        for continent in result.get('continents', []):
            print(f"  - {continent['name']} ({continent['code']})")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


async def example_async_fetch():
    """Example of asynchronous data fetching."""
    print("\n=== Asynchronous Fetch Example ===")
    
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
            print("Countries (first 5):")
            for country in result.get('countries', [])[:5]:
                capital = country.get('capital', 'N/A')
                print(f"  - {country['name']} ({country['code']}) - Capital: {capital}")
            
        except Exception as e:
            print(f"Error: {e}")


def example_data_extraction():
    """Example of data extraction from nested responses."""
    print("\n=== Data Extraction Example ===")
    
    client = GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=False
    )
    
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
            print(f"  - {country['name']} ({country['code']}) - Continent: {continent}")
        
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


if __name__ == "__main__":
    # Run sync example
    example_sync_fetch()
    
    # Run async example
    asyncio.run(example_async_fetch())
    
    # Run data extraction example
    example_data_extraction()
    
    # Run PageInfo example
    example_page_info()
    
    print("\n=== All examples completed successfully! ===")
