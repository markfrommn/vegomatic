"""
GqlFetch-github module for fetching data from the Github GraphQL endpoint with pagination support.
"""

from typing import Any, Dict, List, Optional, Union, Iterator, Callable

from vegomatic.gqlfetch import GqlFetch
class GqlFetchGithub(GqlFetch):
    """
    A GraphQL client for fetching data from the Github GraphQL endpoint with pagination support.
    """

    # The base query for repositories in a Github Organization.
    repo_query_by_owner = """
        query {
            organization(<ORG_ARGS>) {
                repositories(<REPO_ARGS>) {
                    totalCount
                    nodes {
                        name
                        url
                        description
                        createdAt
                        updatedAt
                        id
                        databaseId
                        diskUsage
                        isArchived
                        isDisabled
                        isLocked
                        isPrivate
                        primaryLanguage {
                            name
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

    # The base query for repositories in a Github Organization.
    pr_query_by_repo = """
        query {
            repository(<REPO_ARGS>) {
                name
                url
                pullRequests(<PR_ARGS>, orderBy: { field: CREATED_AT, direction: ASC }) {
                    totalCount
                    nodes {
                        id
                        fullDatabaseId
                        number
                        title
                        state
                        permalink
                        createdAt
                        mergedAt
                        updatedAt
                        closedAt
                        lastEditedAt
                        merged
                        mergedBy {
                            login
                        }
                        author {
                            login
                        }
                        comments (<CIR_ARGS>) {
                            totalCount
                            nodes {
                                url
                                body
                                createdAt
                                updatedAt
                                author {
                                    login
                                }
                                editor {
                                    login
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                        closingIssuesReferences (<CIR_ARGS>) {
                            totalCount
                            nodes {
                                number
                                id
                                title
                                createdAt
                                closed
                                closedAt
                                url
                                comments (<CIR_ARGS>) {
                                    totalCount
                                    nodes {
                                        url
                                        body
                                        createdAt
                                        updatedAt
                                        author {
                                            login
                                        }
                                        editor {
                                            login
                                        }
                                    }
                                    pageInfo {
                                        hasNextPage
                                        endCursor
                                    }
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
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
        super().__init__(endpoint, token=token, headers=headers, use_async=use_async, fetch_schema=fetch_schema, timeout=timeout)

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
            repo_after_arg = f'after: "{after}"'
        if repo_first_arg != "" and repo_after_arg != "":
            comma_arg = ", "

        repo_query_args = f"{repo_first_arg}{comma_arg}{repo_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.repo_query_by_owner.replace("<ORG_ARGS>", query_owner_args)
        query = query.replace("<REPO_ARGS>", repo_query_args)
        return query


    def get_repositories_once(self, organization: str, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Dict[str, Any]]:
        """
        Get a list of repositories for a given Organization.
        """
        query = self.get_repository_query(organization, first, after)
        data = self.fetch_data(query)
        return data

    def get_repositories(self, organization: str, first = 50, progress_cb: Optional[Callable[[int, int], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get a list of repositories for a given Organization.
        """
        repositories = []
        after = None
        while True:
            data = self.get_repositories_once(organization, first, after, ignore_errors)
            repositories.extend(data.get('organization', {}).get('repositories', {}).get('nodes', []))
            if progress_cb is not None:
                progress_cb(len(repositories), data['organization']['repositories']['totalCount'])
            if not data['organization']['repositories']['pageInfo']['hasNextPage']:
                break
            if limit is not None and len(repositories) >= limit:
                break
            after = data['organization']['repositories']['pageInfo']['endCursor']
        return repositories

    def get_pr_query(self, organization: str, repository: str, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a query for a given Repository.
        """
        pr_first_arg = pr_after_arg = cir_first_arg = comma_arg = ""
        query_repo_args = f'owner: "{organization}", name: "{repository}"'

        if (first is not None):
            pr_first_arg = f"first: {first}"
            cir_first_arg = f"first: {first}" # TODO: CIR pagination, punt with first for now
        if (after is not None):
            pr_after_arg = f'after: "{after}"'
        if pr_first_arg != "" and pr_after_arg != "":
            comma_arg = ", "

        pr_query_args = f"{pr_first_arg}{comma_arg}{pr_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.pr_query_by_repo.replace("<REPO_ARGS>", query_repo_args)
        query = query.replace("<PR_ARGS>", pr_query_args)
        query = query.replace("<CIR_ARGS>", cir_first_arg)
        return query

    def get_prs_once(self, organization: str, repository: str, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Dict[str, Any]]:
        """
        Get a list of PRs for a repository
        """
        query = self.get_pr_query(organization, repository, first, after)
        data = self.fetch_data(query)
        return data

    def get_prs(self, organization: str, repository: str, first = 50, progress_cb: Optional[Callable[[int, int], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get a list of PRs for a given Repository.
        """
        prs = []
        after = None
        while True:
            data = self.get_prs_once(organization, repository, first, after, ignore_errors)
            prs.extend(data.get('repository', {}).get('pullRequests', {}).get('nodes', []))
            if progress_cb is not None:
                progress_cb(len(prs), data['repository']['pullRequests']['totalCount'])
            if not data['repository']['pullRequests']['pageInfo']['hasNextPage']:
                break
            if limit is not None and len(prs) >= limit:
                break
            after = data['repository']['pullRequests']['pageInfo']['endCursor']
        return prs

    def clean_prs(self, prs: List[Dict[str, Any]], clean_all: bool = False) -> List[Dict[str, Any]]:
        """
        Clean a list of PRs.

        Delete empty sub-dicts from PRs
        """
        for pr in prs:
            pr_comment_qty = pr.get('comments', {}).get('totalCount', 0)
            if pr_comment_qty == 0 or clean_all:
                del pr['comments']
            pr_cir_qty = pr.get('closingIssuesReferences', {}).get('totalCount', 0)
            if pr_cir_qty == 0 or clean_all:
                del pr['closingIssuesReferences']
        return prs

