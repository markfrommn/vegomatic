"""
GqlFetch module for fetching data from GraphQL endpoints with pagination support.
"""

import asyncio
from dataclasses import dataclass
import os
from typing import Any, Dict, Iterator, List, Optional, Union

from gql import Client, gql
from graphql.error import GraphQLSyntaxError
from gql.dsl import DSLField, DSLFragment, DSLInlineFragment, DSLQuery, DSLSchema
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
import requests

@dataclass
class PageInfo:
    """Represents pagination information from GraphQL responses."""
    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str] = None
    end_cursor: Optional[str] = None


class GqlFetch:
    """
    A GraphQL client for fetching data with pagination support.

    This class provides a high-level interface for fetching data from GraphQL endpoints
    with support for cursor-based pagination and optional DSL query building.
    """

    def __init__(
        self,
        endpoint: str,
        token: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        use_async: bool = False,
        fetch_schema: bool = True,
        timeout: Optional[int] = None
    ):
        """
        Initialize the GqlFetch client.

        Args:
            endpoint: The GraphQL endpoint URL
            headers: Optional headers to include in requests
            use_async: Whether to use async transport (aiohttp) or sync (requests)
            fetch_schema: Whether to fetch the schema from the endpoint
            timeout: Request timeout in seconds
        """
        self.endpoint = endpoint
        self.token = token
        # Try to get token first then key from environment
        if self.token is None:
            self.token = os.getenv("GRAPHQL_TOKEN", default=None)
        self.key = key
        if self.key is None:
            self.key = os.getenv("GRAPHQL_KEY", default=None)
        self.headers = headers or {}
        # Prioritize token over key
        if self.token is not None:
            self.headers["Authorization"] = f"Bearer {self.token}"
        if self.token is None and self.key is not None:
            self.headers["Authorization"] = f"{self.key}"
        self.use_async = use_async
        self.timeout = timeout
        self.fetch_schema = fetch_schema
        self.dsl_schema = None
        self.transport = None
        self.client = None

    def connect(self):
        """
        Connect to the GraphQL endpoint.
        """
        # Create transport
        if self.use_async:
            self.transport = AIOHTTPTransport(
                url=self.endpoint,
                headers=self.headers,
                timeout=self.timeout
            )
        else:
            self.transport = RequestsHTTPTransport(
                url=self.endpoint,
                headers=self.headers,
                timeout=self.timeout
            )

        # Create client
        self.client = Client(
            transport=self.transport,
            fetch_schema_from_transport=self.fetch_schema
        )
        if self.fetch_schema:
            if not self.use_async:
                # This only works with a sync connect
                self.client.connect_sync()
                self.client.session.fetch_schema()
                self.dsl_schema = DSLSchema(self.client.schema)
                self.client.close_sync()
        return

    def set_dsl_schema(self, dsl_schema: DSLSchema = None) -> None:
        """
        Set the DSL schema for building queries dynamically.

        Args:
            schema: The DSL schema instance
        """
        if dsl_schema is None:
            self.dsl_schema = DSLSchema(self.client.schema)
        else:
            self.dsl_schema = dsl_schema

    def _extract_page_info(self, data: Dict[str, Any], page_info_path: str = "pageInfo") -> Optional[PageInfo]:
        """
        Extract PageInfo from GraphQL response.

        Args:
            data: The GraphQL response data
            page_info_path: The path to pageInfo in the response

        Returns:
            PageInfo object or None if not found
        """
        try:
            # Navigate to the pageInfo object
            current = data
            for key in page_info_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None

            if not isinstance(current, dict):
                return None

            return PageInfo(
                has_next_page=current.get('hasNextPage', False),
                has_previous_page=current.get('hasPreviousPage', False),
                start_cursor=current.get('startCursor'),
                end_cursor=current.get('endCursor')
            )
        except (KeyError, TypeError):
            return None

    def _extract_edges(self, data: Dict[str, Any], edges_path: str = "edges") -> List[Dict[str, Any]]:
        """
        Extract edges array from GraphQL response.

        Args:
            data: The GraphQL response data
            edges_path: The path to edges in the response

        Returns:
            List of edge objects
        """
        try:
            # Navigate to the edges array
            current = data
            for key in edges_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return []

            if not isinstance(current, list):
                return []

            return current
        except (KeyError, TypeError):
            return []

    def _extract_nodes(self, data: Dict[str, Any], nodes_path: str = "nodes") -> List[Dict[str, Any]]:
        """
        Extract nodes array from GraphQL response.

        Args:
            data: The GraphQL response data
            nodes_path: The path to nodes in the response

        Returns:
            List of node objects
        """
        try:
            # Navigate to the nodes array
            current = data
            for key in nodes_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return []

            if not isinstance(current, list):
                return []

            return current
        except (KeyError, TypeError):
            return []

    def fetch_data(
        self,
        query: Union[str, DSLQuery],
        variables: Optional[Dict[str, Any]] = None,
        extract_path: Optional[str] = None,
        ignore_errors: bool = False,
        page_info_path: str = "pageInfo",
        edges_path: str = "edges",
        nodes_path: str = "nodes"
    ) -> Dict[str, Any]:
        """
        Fetch data from GraphQL endpoint.

        Args:
            query: GraphQL query string or DSL query object
            variables: Variables to pass with the query
            extract_path: Optional path to extract specific data from response
            page_info_path: Path to pageInfo in the response
            edges_path: Path to edges in the response
            nodes_path: Path to nodes in the response

        Returns:
            Dictionary containing the response data
        """
        if self.use_async:
            raise RuntimeError("Use fetch_data_async for async operations")

        # Execute query
        if isinstance(query, str):
            gql_query = gql(query)
        else:
            gql_query = query
        try:
            result = self.client.execute(gql_query, variable_values=variables)
        except Exception as e:
            if ignore_errors and isinstance(e, requests.exceptions.HTTPError):
                return {}
            elif isinstance(e, GraphQLSyntaxError):
                print(f"GraphQLSyntaxError: {e}")
                if isinstance(query, str):
                    print(f"Query: {query}")
                else:
                    print(f"Query: {gql_query}")
                    print(f"Variables: {variables}")
                raise e
            else:
                raise e

        # Extract data if path specified
        if extract_path:
            current = result
            for key in extract_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    current = None
                    break
            result = current

        return result

    async def fetch_data_async(
        self,
        query: Union[str, DSLQuery],
        variables: Optional[Dict[str, Any]] = None,
        extract_path: Optional[str] = None,
        ignore_errors: bool = False,
        page_info_path: str = "pageInfo",
        edges_path: str = "edges",
        nodes_path: str = "nodes"
    ) -> Dict[str, Any]:
        """
        Fetch data from GraphQL endpoint asynchronously.

        Args:
            query: GraphQL query string or DSL query object
            variables: Variables to pass with the query
            extract_path: Optional path to extract specific data from response
            page_info_path: Path to pageInfo in the response
            edges_path: Path to edges in the response
            nodes_path: Path to nodes in the response

        Returns:
            Dictionary containing the response data
        """
        if not self.use_async:
            raise RuntimeError("Use fetch_data for sync operations")

        # Execute query
        if isinstance(query, str):
            gql_query = gql(query)
        else:
            gql_query = query

        try:
            result = await self.client.execute_async(gql_query, variable_values=variables)
        except Exception as e:
            if ignore_errors and isinstance(e, requests.exceptions.HTTPError):
                return {}
            else:
                raise e

        # Extract data if path specified
        if extract_path:
            current = result
            for key in extract_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    current = None
                    break
            result = current

        return result

    def fetch_paginated(
        self,
        query: Union[str, DSLQuery],
        variables: Optional[Dict[str, Any]] = None,
        cursor_variable: str = "after",
        page_info_path: str = "pageInfo",
        edges_path: str = "edges",
        nodes_path: str = "nodes",
        max_pages: Optional[int] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch data with pagination support.

        Args:
            query: GraphQL query string or DSL query object
            variables: Variables to pass with the query
            cursor_variable: Variable name for the cursor in the query
            page_info_path: Path to pageInfo in the response
            edges_path: Path to edges in the response
            nodes_path: Path to nodes in the response
            max_pages: Maximum number of pages to fetch (None for unlimited)

        Yields:
            Lists of objects from each page
        """
        if self.use_async:
            raise RuntimeError("Use fetch_paginated_async for async operations")

        current_variables = variables.copy() if variables else {}
        page_count = 0

        while True:
            if max_pages and page_count >= max_pages:
                break

            # Execute query
            result = self.fetch_data(query, current_variables)

            # Extract page info
            page_info = self._extract_page_info(result, page_info_path)

            # Extract data (try edges first, then nodes)
            data = self._extract_edges(result, edges_path)
            if not data:
                data = self._extract_nodes(result, nodes_path)

            if data:
                yield data
                page_count += 1
            else:
                break

            # Check if there's a next page
            if not page_info or not page_info.has_next_page:
                break

            # Set cursor for next page
            if page_info.end_cursor:
                current_variables[cursor_variable] = page_info.end_cursor
            else:
                break

    async def fetch_paginated_async(
        self,
        query: Union[str, DSLQuery],
        variables: Optional[Dict[str, Any]] = None,
        cursor_variable: str = "after",
        page_info_path: str = "pageInfo",
        edges_path: str = "edges",
        nodes_path: str = "nodes",
        max_pages: Optional[int] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch data with pagination support asynchronously.

        Args:
            query: GraphQL query string or DSL query object
            variables: Variables to pass with the query
            cursor_variable: Variable name for the cursor in the query
            page_info_path: Path to pageInfo in the response
            edges_path: Path to edges in the response
            nodes_path: Path to nodes in the response
            max_pages: Maximum number of pages to fetch (None for unlimited)

        Yields:
            Lists of objects from each page
        """
        if not self.use_async:
            raise RuntimeError("Use fetch_paginated for sync operations")

        current_variables = variables.copy() if variables else {}
        page_count = 0

        while True:
            if max_pages and page_count >= max_pages:
                break

            # Execute query
            result = await self.fetch_data_async(query, current_variables)

            # Extract page info
            page_info = self._extract_page_info(result, page_info_path)

            # Extract data (try edges first, then nodes)
            data = self._extract_edges(result, edges_path)
            if not data:
                data = self._extract_nodes(result, nodes_path)

            if data:
                yield data
                page_count += 1
            else:
                break

            # Check if there's a next page
            if not page_info or not page_info.has_next_page:
                break

            # Set cursor for next page
            if page_info.end_cursor:
                current_variables[cursor_variable] = page_info.end_cursor
            else:
                break

    def create_dsl_query(self, query_name: str, **kwargs) -> DSLQuery:
        """
        Create a DSL query using the schema.

        Args:
            query_name: Name of the query to create
            **kwargs: Additional arguments for the query

        Returns:
            DSLQuery object

        Raises:
            RuntimeError: If DSL schema is not set
        """
        if not self.dsl_schema:
            raise RuntimeError("DSL schema not set. Call set_dsl_schema() first.")

        return self.dsl_schema.query(query_name, **kwargs)

    def close(self) -> None:
        """Close the client and transport."""
        if hasattr(self.transport, 'close'):
            self.transport.close()

    async def aclose(self) -> None:
        """Close the client and transport asynchronously."""
        if hasattr(self.transport, 'aclose'):
            await self.transport.aclose()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
