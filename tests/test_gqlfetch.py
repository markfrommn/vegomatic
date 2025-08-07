#!/usr/bin/env python3
"""
Tests for the GqlFetch module.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any

from vegomatic.gqlfetch import GqlFetch, PageInfo


class TestPageInfo:
    """Test the PageInfo dataclass."""
    
    def test_page_info_creation(self):
        """Test creating a PageInfo instance."""
        page_info = PageInfo(
            has_next_page=True,
            has_previous_page=False,
            start_cursor="start123",
            end_cursor="end456"
        )
        
        assert page_info.has_next_page is True
        assert page_info.has_previous_page is False
        assert page_info.start_cursor == "start123"
        assert page_info.end_cursor == "end456"
    
    def test_page_info_defaults(self):
        """Test PageInfo with default values."""
        page_info = PageInfo(
            has_next_page=False,
            has_previous_page=True
        )
        
        assert page_info.start_cursor is None
        assert page_info.end_cursor is None


class TestGqlFetch:
    """Test the GqlFetch class."""
    
    def test_init_sync(self):
        """Test GqlFetch initialization with sync transport."""
        client = GqlFetch(
            endpoint="https://example.com/graphql",
            headers={"Authorization": "Bearer token"},
            use_async=False,
            timeout=30
        )
        
        assert client.endpoint == "https://example.com/graphql"
        assert client.headers == {"Authorization": "Bearer token"}
        assert client.use_async is False
        assert client.timeout == 30
        assert client.dsl_schema is None
        
        client.close()
    
    def test_init_async(self):
        """Test GqlFetch initialization with async transport."""
        client = GqlFetch(
            endpoint="https://example.com/graphql",
            use_async=True
        )
        
        assert client.use_async is True
        
        client.close()
    
    def test_set_dsl_schema(self):
        """Test setting DSL schema."""
        client = GqlFetch("https://example.com/graphql")
        
        mock_schema = Mock()
        client.set_dsl_schema(mock_schema)
        
        assert client.dsl_schema == mock_schema
        
        client.close()
    
    def test_extract_page_info(self):
        """Test page info extraction."""
        client = GqlFetch("https://example.com/graphql")
        
        # Test with valid page info
        data = {
            "data": {
                "repositories": {
                    "pageInfo": {
                        "hasNextPage": True,
                        "hasPreviousPage": False,
                        "startCursor": "start123",
                        "endCursor": "end456"
                    }
                }
            }
        }
        
        page_info = client._extract_page_info(data, "data.repositories.pageInfo")
        assert page_info is not None
        assert page_info.has_next_page is True
        assert page_info.has_previous_page is False
        assert page_info.start_cursor == "start123"
        assert page_info.end_cursor == "end456"
        
        # Test with missing page info
        page_info = client._extract_page_info(data, "nonexistent.path")
        assert page_info is None
        
        client.close()
    
    def test_extract_edges(self):
        """Test edges extraction."""
        client = GqlFetch("https://example.com/graphql")
        
        # Test with valid edges
        data = {
            "data": {
                "repositories": {
                    "edges": [
                        {"node": {"name": "repo1"}},
                        {"node": {"name": "repo2"}}
                    ]
                }
            }
        }
        
        edges = client._extract_edges(data, "data.repositories.edges")
        assert len(edges) == 2
        assert edges[0]["node"]["name"] == "repo1"
        assert edges[1]["node"]["name"] == "repo2"
        
        # Test with missing edges
        edges = client._extract_edges(data, "nonexistent.path")
        assert edges == []
        
        client.close()
    
    def test_extract_nodes(self):
        """Test nodes extraction."""
        client = GqlFetch("https://example.com/graphql")
        
        # Test with valid nodes
        data = {
            "data": {
                "repositories": {
                    "nodes": [
                        {"name": "repo1"},
                        {"name": "repo2"}
                    ]
                }
            }
        }
        
        nodes = client._extract_nodes(data, "data.repositories.nodes")
        assert len(nodes) == 2
        assert nodes[0]["name"] == "repo1"
        assert nodes[1]["name"] == "repo2"
        
        # Test with missing nodes
        nodes = client._extract_nodes(data, "nonexistent.path")
        assert nodes == []
        
        client.close()
    
    @patch('vegomatic.gqlfetch.gql')
    def test_fetch_data_sync(self, mock_gql):
        """Test sync data fetching."""
        client = GqlFetch("https://example.com/graphql", use_async=False)
        
        # Mock the client execution
        mock_client = Mock()
        mock_client.execute.return_value = {"data": {"result": "test"}}
        client.client = mock_client
        
        # Mock gql function
        mock_gql_query = Mock()
        mock_gql.return_value = mock_gql_query
        
        result = client.fetch_data("query { test }")
        
        assert result == {"data": {"result": "test"}}
        mock_client.execute.assert_called_once_with(mock_gql_query, variable_values=None)
        
        client.close()
    
    @patch('vegomatic.gqlfetch.gql')
    def test_fetch_data_with_extraction(self, mock_gql):
        """Test data fetching with extraction path."""
        client = GqlFetch("https://example.com/graphql", use_async=False)
        
        # Mock the client execution
        mock_client = Mock()
        mock_client.execute.return_value = {
            "data": {
                "repositories": {
                    "nodes": [{"name": "repo1"}]
                }
            }
        }
        client.client = mock_client
        
        # Mock gql function
        mock_gql_query = Mock()
        mock_gql.return_value = mock_gql_query
        
        result = client.fetch_data(
            "query { repositories { nodes { name } } }",
            extract_path="data.repositories.nodes"
        )
        
        assert result == [{"name": "repo1"}]
        
        client.close()
    
    def test_fetch_data_async_error(self):
        """Test that sync fetch_data raises error when used with async client."""
        client = GqlFetch("https://example.com/graphql", use_async=True)
        
        with pytest.raises(RuntimeError, match="Use fetch_data_async for async operations"):
            client.fetch_data("query { test }")
        
        client.close()
    
    def test_fetch_data_async_sync_error(self):
        """Test that async fetch_data_async raises error when used with sync client."""
        client = GqlFetch("https://example.com/graphql", use_async=False)
        
        with pytest.raises(RuntimeError, match="Use fetch_data for sync operations"):
            asyncio.run(client.fetch_data_async("query { test }"))
        
        client.close()
    
    def test_create_dsl_query_without_schema(self):
        """Test creating DSL query without schema set."""
        client = GqlFetch("https://example.com/graphql")
        
        with pytest.raises(RuntimeError, match="DSL schema not set"):
            client.create_dsl_query("test")
        
        client.close()
    
    def test_create_dsl_query_with_schema(self):
        """Test creating DSL query with schema set."""
        client = GqlFetch("https://example.com/graphql")
        
        mock_schema = Mock()
        mock_query = Mock()
        mock_schema.query.return_value = mock_query
        
        client.set_dsl_schema(mock_schema)
        
        result = client.create_dsl_query("test", arg1="value1")
        
        assert result == mock_query
        mock_schema.query.assert_called_once_with("test", arg1="value1")
        
        client.close()
    
    def test_context_manager_sync(self):
        """Test sync context manager."""
        with GqlFetch("https://example.com/graphql", use_async=False) as client:
            assert isinstance(client, GqlFetch)
    
    @pytest.mark.asyncio
    async def test_context_manager_async(self):
        """Test async context manager."""
        async with GqlFetch("https://example.com/graphql", use_async=True) as client:
            assert isinstance(client, GqlFetch)


class TestGqlFetchPagination:
    """Test pagination functionality."""
    
    def test_fetch_paginated_sync(self):
        """Test sync paginated fetching."""
        client = GqlFetch("https://example.com/graphql", use_async=False)
        
        # Mock the client execution
        mock_client = Mock()
        client.client = mock_client
        
        # Mock responses for pagination
        responses = [
            {
                "data": {
                    "repositories": {
                        "pageInfo": {
                            "hasNextPage": True,
                            "endCursor": "cursor1"
                        },
                        "edges": [
                            {"node": {"name": "repo1"}},
                            {"node": {"name": "repo2"}}
                        ]
                    }
                }
            },
            {
                "data": {
                    "repositories": {
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": "cursor2"
                        },
                        "edges": [
                            {"node": {"name": "repo3"}}
                        ]
                    }
                }
            }
        ]
        
        mock_client.execute.side_effect = responses
        
        # Mock gql function
        with patch('vegomatic.gqlfetch.gql') as mock_gql:
            mock_gql_query = Mock()
            mock_gql.return_value = mock_gql_query
            
            pages = list(client.fetch_paginated(
                "query getRepos($after: String) { repositories(after: $after) { ... } }",
                max_pages=2
            ))
            
            assert len(pages) == 2
            assert len(pages[0]) == 2  # First page has 2 items
            assert len(pages[1]) == 1  # Second page has 1 item
            
            # Check that variables were updated for pagination
            assert mock_client.execute.call_count == 2
        
        client.close()
    
    def test_fetch_paginated_async_error(self):
        """Test that sync fetch_paginated raises error when used with async client."""
        client = GqlFetch("https://example.com/graphql", use_async=True)
        
        with pytest.raises(RuntimeError, match="Use fetch_paginated_async for async operations"):
            list(client.fetch_paginated("query { test }"))
        
        client.close()


if __name__ == "__main__":
    pytest.main([__file__])
