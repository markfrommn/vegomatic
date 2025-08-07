"""
GqlFetch-github module for fetching data from the Github GraphQL endpoint with pagination support.
"""

from .gqlfetch import GqlFetch


class GqlFetchGithub(GqlFetch):
    """
    A GraphQL client for fetching data from the Github GraphQL endpoint with pagination support.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        use_async: bool = False,
        fetch_schema: bool = True,
        timeout: Optional[int] = None
    ):
        """
        Initialize the GqlFetchGithub client.
        """
        if endpoint is None:
            endpoint = "https://api.github.com/graphql"
        super().__init__(endpoint, token, headers, use_async, fetch_schema, timeout)

    def connect(self):
        """
        Connect to the Github GraphQL endpoint.
        """
        super().connect()

    def close(self):
        """
        Close the connection to the Github GraphQL endpoint.
        """
        super().close()