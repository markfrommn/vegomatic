# GqlFetch Module

The `GqlFetch` module provides a high-level interface for fetching data from GraphQL endpoints with support for pagination, async operations, and optional DSL query building.

## Features

- **GraphQL Data Fetching**: Execute GraphQL queries against any GraphQL endpoint
- **Pagination Support**: Automatic handling of cursor-based pagination with PageInfo
- **Async/Sync Support**: Choose between synchronous and asynchronous operations
- **DSL Integration**: Optional support for building queries using the GraphQL DSL
- **Flexible Data Extraction**: Extract data from nested response structures
- **Context Manager Support**: Use with `with` statements for automatic cleanup

## Installation

The module requires the `gql` library with all optional dependencies:

```bash
pip install "gql[all]>=3.5.0"
```

## Basic Usage

### Simple Data Fetching

```python
from vegomatic.gqlfetch import GqlFetch

# Create a client
client = GqlFetch(
    endpoint="https://countries.trevorblades.com/",
    use_async=False
)

# Define a query
query = """
query getContinents {
  continents {
    code
    name
  }
}
"""

# Fetch data
result = client.fetch_data(query)
print(result)

client.close()
```

### Async Data Fetching

```python
import asyncio
from vegomatic.gqlfetch import GqlFetch

async def fetch_countries():
    async with GqlFetch(
        endpoint="https://countries.trevorblades.com/",
        use_async=True
    ) as client:
        
        query = """
        query getCountries {
          countries {
            code
            name
            capital
          }
        }
        """
        
        result = await client.fetch_data_async(query)
        return result

# Run the async function
result = asyncio.run(fetch_countries())
```

## Pagination Support

The module supports cursor-based pagination using the Relay specification:

```python
from vegomatic.gqlfetch import GqlFetch

client = GqlFetch(
    endpoint="https://api.github.com/graphql",
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

# Query with pagination
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

# Fetch all pages
for page_num, page_data in enumerate(client.fetch_paginated(query, max_pages=5)):
    print(f"Page {page_num + 1}:")
    for repo in page_data:
        node = repo.get('node', {})
        print(f"  - {node.get('name')}: {node.get('stargazerCount')} stars")
```

## Data Extraction

You can extract specific data from nested response structures:

```python
client = GqlFetch("https://example.com/graphql")

query = """
query getData {
  user {
    repositories {
      nodes {
        name
        description
      }
    }
  }
}
"""

# Extract only the repositories array
repositories = client.fetch_data(
    query,
    extract_path="user.repositories.nodes"
)

print(repositories)  # List of repository objects
```

## DSL Integration

The module supports building queries using the GraphQL DSL:

```python
from gql.dsl import DSLSchema
from vegomatic.gqlfetch import GqlFetch

client = GqlFetch("https://example.com/graphql")

# Set up DSL schema (you would typically get this from the GraphQL schema)
schema = DSLSchema(client.client.schema)
client.set_dsl_schema(schema)

# Create a query using DSL
query = client.create_dsl_query("getRepositories", first=10)

# Execute the query
result = client.fetch_data(query)
```

## Configuration Options

### Client Initialization

```python
client = GqlFetch(
    endpoint="https://api.example.com/graphql",
    headers={
        "Authorization": "Bearer token",
        "Content-Type": "application/json"
    },
    use_async=False,  # True for async operations
    fetch_schema=True,  # Whether to fetch schema from endpoint
    timeout=30  # Request timeout in seconds
)
```

### Pagination Configuration

```python
# Custom pagination paths
for page_data in client.fetch_paginated(
    query,
    cursor_variable="after",  # Variable name for cursor
    page_info_path="data.repositories.pageInfo",  # Path to pageInfo
    edges_path="data.repositories.edges",  # Path to edges
    nodes_path="data.repositories.nodes",  # Path to nodes
    max_pages=10  # Maximum pages to fetch
):
    print(page_data)
```

## PageInfo Class

The `PageInfo` dataclass represents pagination information:

```python
from vegomatic.gqlfetch import PageInfo

page_info = PageInfo(
    has_next_page=True,
    has_previous_page=False,
    start_cursor="start123",
    end_cursor="end456"
)
```

## Error Handling

The module provides clear error messages for common issues:

```python
try:
    result = client.fetch_data(query)
except RuntimeError as e:
    if "Use fetch_data_async" in str(e):
        # You're using sync method with async client
        result = await client.fetch_data_async(query)
    elif "Use fetch_data" in str(e):
        # You're using async method with sync client
        result = client.fetch_data(query)
except Exception as e:
    print(f"GraphQL error: {e}")
```

## Context Manager Usage

Use the client as a context manager for automatic cleanup:

```python
# Sync usage
with GqlFetch("https://example.com/graphql") as client:
    result = client.fetch_data(query)

# Async usage
async with GqlFetch("https://example.com/graphql", use_async=True) as client:
    result = await client.fetch_data_async(query)
```

## Advanced Examples

### Custom Transport Configuration

```python
from gql.transport.aiohttp import AIOHTTPTransport

# Create custom transport
transport = AIOHTTPTransport(
    url="https://api.example.com/graphql",
    headers={"Authorization": "Bearer token"},
    timeout=60
)

# Use with GqlFetch
client = GqlFetch(
    endpoint="https://api.example.com/graphql",
    use_async=True
)
client.transport = transport
```

### Batch Processing

```python
# Process large datasets in batches
all_data = []
for page_data in client.fetch_paginated(query, max_pages=None):
    all_data.extend(page_data)
    print(f"Processed {len(all_data)} items so far")
```

### Error Recovery

```python
def fetch_with_retry(client, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            return client.fetch_data(query)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
```

## Testing

The module includes comprehensive tests:

```bash
pytest tests/test_gqlfetch.py
```

## Dependencies

- `gql[all]>=3.5.0`: GraphQL client library
- `aiohttp`: For async transport (included in gql[all])
- `requests`: For sync transport (included in gql[all])

## License

This module is part of the vegomatic package and follows the same license terms.
