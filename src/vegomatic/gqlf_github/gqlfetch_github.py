"""
GqlFetch-github module for fetching data from the Github GraphQL endpoint with pagination support.
"""

from typing import Any, Dict, List, Optional, Union, Iterator, Callable

from vegomatic.gqlfetch import GqlFetch
class GqlFetchGithub(GqlFetch):
    """
    A GraphQL client for fetching data from the Github GraphQL endpoint with pagination support.
    """

    repo_query_by_owner = """
        query {
            organization({org_args}) {
                repositories({repo_args}) { 
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


    def get_repository_query(self, organization: str, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a query for a given Organization.
        """
        repo_first_arg = repo_after_arg = comma_arg = ""
        query_owner_args = f'login: "{organization}"'

        if (first is not None):
            repo_first_arg = f"first: {first}"
        if (after is not None):
            repo_after_arg = f"after: {after}"
        if repo_first_arg != "" and repo_after_arg != "":
            comma_arg = ", "

        repo_query_args = f"{repo_first_arg}{comma_arg}{repo_after_arg}"
        query = self.repo_query_by_owner.format(org_args=query_owner_args, repo_args=repo_query_args)
        return query

    def get_repositories_once(self, organization: str, first: int = 50, after: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get a list of repositories for a given Organization.
        """
        query = self.get_repository_query(organization, first, after)
        data = self.fetch_data(query)
        return data

    def get_repositories(self, organization: str, first = 50, progress_cb: Optional[Callable[[int, int], None]] = None) -> List[Dict[str, Any]]:
        """
        Get a list of repositories for a given Organization.
        """
        repositories = []
        after = None
        while True:
            data = self.get_repositories_once(organization, first, after)
            repositories.extend(data.get('organization', {}).get('repositories', {}).get('nodes', []))
            if progress_cb is not None:
                progress_cb(len(repositories), data['organization']['repositories']['totalCount'])
            if not data['pageInfo']['hasNextPage']:
                break
            after = data['pageInfo']['endCursor']
        return repositories


