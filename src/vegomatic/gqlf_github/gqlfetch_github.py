"""
GqlFetch-github module for fetching data from the Github GraphQL endpoint with pagination support.
"""

from typing import Any, Callable, Dict, List, Mapping, Optional

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

    @classmethod
    def pr_permalink_to_name(cls, prpermalink: str) -> str:
        """
        Get a name for a PR.

        Permalink is like https://github.com/org/repo/pull/1234

        Chop the prefix, the /pull and convert / to -
        """
        prname = prpermalink.replace("https://github.com/", "").replace("/pull", "").replace("/", "-")
        return prname

    @classmethod
    def clean_pr(cls, pr: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Clean a single PR.
        Delete empty sub-dicts and hasNextPage false pageInfo from PRs
        """
        if pr.get('comments', {}):
          if (len(pr['comments']['nodes']) == 0):
            del pr['comments']
          elif pr['comments'].get('pageInfo', {}).get('hasNextPage') is False:
            del pr['comments']['pageInfo']
        if pr.get('closingIssuesReferences', {}):
          if pr['closingIssuesReferences'].get('comments', {}):
            if (len(pr['closingIssuesReferences'].get('comments', {}).get('nodes', [])) == 0):
                del pr['closingIssuesReferences']['comments']
            elif pr['closingIssuesReferences']['comments'].get('pageInfo', {}).get('hasNextPage') is False:
                del pr['closingIssuesReferences']['comments']['pageInfo']
          if (len(pr['closingIssuesReferences'].get('nodes', [])) == 0):
            del pr['closingIssuesReferences']
          elif pr['closingIssuesReferences'].get('pageInfo', {}).get('hasNextPage') is False:
            del pr['closingIssuesReferences']['pageInfo']

        return pr

    @classmethod
    def clean_prs(cls, prs: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        """
        Clean a list of PRs.

        Delete empty sub-dicts and hasNextPage false from PRs
        """
        for id in prs:
            pr = cls.clean_pr(prs[id])
            prs[id] = pr
        return prs

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

    def get_repositories(self, organization: str, first = 50, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get a list of repositories for a given Organization.
        """
        repositories = {}
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            data = self.get_repositories_once(organization, first, after, ignore_errors)
            for repo in data.get('organization', {}).get('repositories', {}).get('nodes', []):
                repositories[repo['name']] = repo
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

    def get_repo_prs_once(self, organization: str, repository: str, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Dict[str, Any]]:
        """
        Get a list of PRs for a repository
        """
        query = self.get_pr_query(organization, repository, first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_repo_prs(self, organization: str, repository: str, first = 50, batch_cb: Optional[Callable[[Mapping[str, Any], str, str, str], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get a list of PRs for a given Repository.
        """
        prs = {}
        # Reset the issues dict if we are using a batch callback
        if batch_cb is not None:
          prs = {}
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            data = self.get_repo_prs_once(organization, repository, first, after, ignore_errors)
            for pr in data.get('repository', {}).get('pullRequests', {}).get('nodes', []):
                prname = self.pr_permalink_to_name(pr['permalink'])
                prs[prname] = pr
            if batch_cb is not None:
                endCursor = data['repository']['pullRequests']['pageInfo']['endCursor']
                batch_cb(prs, organization, repository, endCursor)
                # Reset the prs dict for the next batch if using the callback and/or so we return an empty dict
                prs = {}
            if not data['repository']['pullRequests']['pageInfo']['hasNextPage']:
                break
            if limit is not None and len(prs) >= limit:
                break
            after = data['repository']['pullRequests']['pageInfo']['endCursor']
        return prs


