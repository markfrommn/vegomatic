"""
GqlFetch-linear module for fetching data from the Linear GraphQL endpoint with pagination support.
"""

from typing import Any, Callable, Dict, List, Mapping, Optional

from vegomatic.gqlfetch import GqlFetch
class GqlFetchLinear(GqlFetch):
    """
    A GraphQL query for fetching Teams from the Linear.app GraphQL endpoint with pagination support.
    """
    # The base query for teams in a Linear Organization.
    team_query = """
      query Teams {
        teams {
          nodes {
            id
            key
            displayName
            name
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    """
    # Sample TEAM ARGS = id: "ff155ab0-e5b3-4373-9090-0ddde594c034") {
    # We keep this short because we have to refetch by issue anyway
    issue_query_by_team = """
      query Team {
        team(<TEAM_ARGS>) {
          id
          name
          issues (<ISSUES_ARGS>) {
            nodes {
              id
              identifier
              createdAt
              startedAt
              completedAt
              title
              description
              url
            }
            pageInfo {
                hasNextPage
                endCursor
            }
          }
        }
      }
    """

    issue_subquery_children = """
          children (<CHILDREN_ARGS>) {
            nodes {
              id
              identifier
              description
            }
            pageInfo {
                hasNextPage
                endCursor
            }
          }
    """
    issue_subquery_inverse_relations = """
         inverseRelations (<INVERSE_RELATIONS_ARGS>) {
            nodes {
              id
              type
              issue {
                id
                identifier
              }
              relatedIssue {
                id
                identifier
              }
            }
            pageInfo {
                hasNextPage
                endCursor
            }
          }
    """
    issue_subquery_relations = """
          relations (<RELATIONS_ARGS>) {
            nodes {
              id
              type
              issue {
                id
                identifier
              }
              relatedIssue {
                id
                identifier
              }
            }
            pageInfo {
                hasNextPage
                endCursor
            }
          }
    """
    issue_subquery_history = """
          history (<HISTORY_ARGS>) {
            nodes {
              attachment {
                id
                url
                title
              }
              actor {
                id
                name
                displayName
              }
              createdAt
              fromCycle {
                name
              }
              toCycle {
                id
                name
              }
              fromState {
                id
                name
              }
              toState {
                id
                name
              }
              fromAssignee {
                id
                name
                displayName
              }
              toAssignee {
                id
                name
                displayName
              }
              changes
            }
            pageInfo {
                hasNextPage
                endCursor
            }
          }

    """
    # Sample ISSUE_ARGS: id: "BLD-832"
    issue_query_all_data = """
      query Issue {
        issue(<ISSUE_ARGS>) {
          id
          identifier
          createdAt
          startedAt
          completedAt
          title
          description
          url
          activitySummary
          parent {
            id
            identifier
          }
          <SUBQUERY_CHILDREN>
          <SUBQUERY_INVERSE_RELATIONS>
          <SUBQUERY_RELATIONS>
          <SUBQUERY_HISTORY>
        }
      }
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        use_async: bool = False,
        fetch_schema: bool = True,
        timeout: Optional[int] = None
    ):
        """
        Initialize the GqlFetchLinear client.

        GraphQL Key is used for Linear.app (not token) - base class will DTRT and leave out Bearer
        """
        if endpoint is None:
            endpoint = "https://api.linear.app/graphql"
        super().__init__(endpoint, key=key, headers=headers, use_async=use_async, fetch_schema=fetch_schema, timeout=timeout)

    @classmethod
    def nested_get(indict: Dict, keys: List[str]) -> Any:
      """
      Get a nested property from a dict.
      """
      outvalue = indict
      for key in keys:
          outvalue = outvalue[key]
      return outvalue

    @classmethod
    def replace_or_append_field(cls, adict: Mapping[str, Any], key: str, newStuff: Mapping[str, Any] | List[Any], subProps: Optional[List[str]] = None) -> Mapping[str, Any]:
        """
        Replace or append a new value to an existing List field in a dict.

        If the field is not in the dict or is None, add the field with the new value.
        If the field is a list, append the newStuff to the list.
        If the field in the dict is not a list, raise a TypeError.

        act[key] and newStuff must be lists or much be a dicts with matching sub-properties referenced by subProps that are lists.
        """
        if newStuff is None or len(newStuff) == 0:
          return adict
        if key not in adict or adict[key] is None:
          adict[key] = newStuff
        else:
          if subProps is None:
            toExtend = adict[key]
            toAdd = newStuff
          else:
            toExtend = cls.nested_get(adict[key], subProps)
            toAdd = cls.nested_get(newStuff, subProps)
          if not isinstance(toExtend, list) or not isinstance(toAdd, list):
            raise TypeError("property {} or newStuff not a list".format(key))
          toExtend.extend(toAdd)
        return adict

    @classmethod
    def clean_issue(cls, issue: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Clean a single Issue.
        """
        if issue.get('children', {}).get('nodes', []):
          if (len(issue.get('children', {}).get('nodes', [])) == 0):
            del issue['children']
        if issue.get('inverseRelations', {}).get('nodes', []):
          if (len(issue.get('inverseRelations', {}).get('nodes', [])) == 0):
            del issue['inverseRelations']
        if issue.get('relations', {}).get('nodes', []):
          if (len(issue.get('relations', {}).get('nodes', [])) == 0):
            del issue['relations']
        if issue.get('history', {}).get('nodes', []):
          if (len(issue.get('history', {}).get('nodes', [])) == 0):
            del issue['history']
        return issue

    @classmethod
    def clean_issues(cls, issues: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        """
        Clean a list of Issues.

        Delete empty sub-dicts from Issues
        """
        for id in issues:
            issue = cls.clean_issue(issues[id])
            issues[id] = issue
        return issues

    def connect(self):
        """
        Connect to the Linear GraphQL endpoint.
        """
        super().connect()

    def close(self):
        """
        Close the connection to the Linear GraphQL endpoint.
        """
        super().close()

    def get_team_query(self, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a query for a teams visible to the user.
        """
        team_first_arg = team_after_arg = comma_arg = ""

        if (first is not None):
            team_first_arg = f"first: {first}"
        if (after is not None):
            team_after_arg = f'after: "{after}"'
        if team_first_arg != "" and team_after_arg != "":
            comma_arg = ", "

        team_query_args = f"{team_first_arg}{comma_arg}{team_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.team_query.replace("<TEAM_ARGS>", team_query_args)
        return query

    def get_issues_query(self, team: str, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a issues query for a given Team.
        """
        issue_first_arg = issue_after_arg = comma_arg = ""
        query_team_arg = f'id: "{team}"'

        if (first is not None):
            issue_first_arg = f"first: {first}"
        if (after is not None):
            issue_after_arg = f'after: "{after}"'
        if issue_first_arg != "" and issue_after_arg != "":
            comma_arg = ", "

        issue_query_args = f"{issue_first_arg}{comma_arg}{issue_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.issue_query_by_team.replace("<TEAM_ARGS>", query_team_arg)
        query = query.replace("<ISSUES_ARGS>", issue_query_args)
        return query

    def get_issue_query(self, issue: str,
                        children_first: int = 50, children_after: Optional[str] = None,
                        inverse_relations_first: int = 50, inverse_relations_after: Optional[str] = None,
                        relations_first: int = 50, relations_after: Optional[str] = None,
                        history_first: int = 50, history_after: Optional[str] = None) -> str:
        """
        Get a query for everything about a given Issue.
        """
        query = self.issue_query_all_data

        # Initialize all the GraphQL arguments to empty strings
        history_first_arg = history_after_arg = history_comma_arg = ""
        children_first_arg = children_after_arg = children_comma_arg = ""
        inverse_relations_first_arg = inverse_relations_after_arg = inverse_relations_comma_arg = ""
        relations_first_arg = relations_after_arg = relations_comma_arg = ""

        query_issue_args = f'id: "{issue}"'
        # History - fill in or delete
        if history_first is not None and history_first > 0:
          query = query.replace("<SUBQUERY_HISTORY>", self.issue_subquery_history)
          history_first_arg = f"first: {history_first}"
          if history_after is not None:
              history_after_arg = f'after: "{history_after}"'
          if history_first_arg != "" and history_after_arg != "":
              history_comma_arg = ","
        else:
          query = query.replace("<SUBQUERY_HISTORY>", "")
        # Children - fill in or delete
        if children_first is not None and children_first > 0:
          query = query.replace("<SUBQUERY_CHILDREN>", self.issue_subquery_children)
          children_first_arg = f"first: {children_first}"
          if children_after is not None:
              children_after_arg = f'after: "{children_after}"'
          if children_first_arg != "" and children_after_arg != "":
              children_comma_arg = ","
        else:
          query = query.replace("<SUBQUERY_CHILDREN>", "")
        # Inverse Relations - fill in or delete
        if inverse_relations_first is not None and inverse_relations_first > 0:
          query = query.replace("<SUBQUERY_INVERSE_RELATIONS>", self.issue_subquery_inverse_relations)
          inverse_relations_first_arg = f"first: {inverse_relations_first}"
          if inverse_relations_after is not None:
            inverse_relations_after_arg = f'after: "{inverse_relations_after}"'
          if inverse_relations_first_arg != "" and inverse_relations_after_arg != "":
              inverse_relations_comma_arg = ","
        else:
          query = query.replace("<SUBQUERY_INVERSE_RELATIONS>", "")
        # Relations - fill in or delete
        if relations_first is not None and relations_first > 0:
          query = query.replace("<SUBQUERY_RELATIONS>", self.issue_subquery_relations)
          relations_first_arg = f"first: {relations_first}"
          if relations_after is not None:
              relations_after_arg = f'after: "{relations_after}"'
          if relations_first_arg != "" and relations_after_arg != "":
              relations_comma_arg = ","
        else:
          query = query.replace("<SUBQUERY_RELATIONS>", "")

        children_query_args = f"{children_first_arg}{children_comma_arg}{children_after_arg}"
        inverse_relations_query_args = f"{inverse_relations_first_arg}{inverse_relations_comma_arg}{inverse_relations_after_arg}"
        relations_query_args = f"{relations_first_arg}{relations_comma_arg}{relations_after_arg}"
        history_query_args = f"{history_first_arg}{history_comma_arg}{history_after_arg}"

        # We can't use format() here because the query is filled with curly braces
        query = query.replace("<ISSUE_ARGS>", query_issue_args)
        query = query.replace("<CHILDREN_ARGS>", children_query_args)
        query = query.replace("<INVERSE_RELATIONS_ARGS>", inverse_relations_query_args)
        query = query.replace("<RELATIONS_ARGS>", relations_query_args)
        query = query.replace("<HISTORY_ARGS>", history_query_args)
        return query

    def get_teams_once(self, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get a list of teams without for one batch.
        """
        query = self.get_team_query(first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_teams(self, first = 50, progress_cb: Optional[Callable[[int, int], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        """
        Get a list of teams, iterating over all pages.
        """
        teams = []
        after = None
        while True:
            data = self.get_teams_once(first, after, ignore_errors)
            teams.extend(data.get('teams', {}).get('nodes', []))
            # Linear does not have a totalCount, so just call with a count and -1
            if progress_cb is not None:
                progress_cb(len(teams), -1)
            if not data['teams']['pageInfo']['hasNextPage']:
                break
            if limit is not None and len(teams) >= limit:
                break # TODO: Add a limit to the query
            after = data['teams']['pageInfo']['endCursor']
        return data.get('teams', {}).get('nodes', [])

    def get_issues_once(self, team: str, issues: Mapping[str, Any] = None, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get a list of Issues for a team
        """
        if issues is None:
          issues = {}
        query = self.get_issues_query(team, first, after)
        data = self.fetch_data(query)
        for issue in data.get('team', {}).get('issues', {}).get('nodes', []):
            issues[issue['identifier']] = issue
        return data

    def get_issues(self, team: str, issues: Mapping[str, Any] = None, first = 50, progress_cb: Optional[Callable[[int, int], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        """
        Get a list of Issues for a given Team.

        We build this as a dict of issues, keyed by issue id do that we can merge the data from get_issue_all_data() later.
        """
        if issues is None:
          issues = {}
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            data = self.get_issues_once(team, issues, first, after, ignore_errors)
            for issue in data.get('team', {}).get('issues', {}).get('nodes', []):
                # Flag that we are incomplete so we can replace later instead of having to merge
                issue["is_full"] = False
                issues[issue['identifier']] = issue
            if progress_cb is not None:
                progress_cb(len(issues), -1)
            if not data['team']['issues']['pageInfo']['hasNextPage']:
                break
            if limit is not None and len(issues) >= limit:
                break
            after = data['team']['issues']['pageInfo']['endCursor']
        return issues

    def get_issue_all_data_once(self, issue: Mapping[str, Any],
                                children_first: int = 50, children_after: Optional[str] = None,
                                inverse_relations_first: int = 50, inverse_relations_after: Optional[str] = None,
                                relations_first: int = 50, relations_after: Optional[str] = None,
                                history_first: int = 50, history_after: Optional[str] = None,
                                ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get all data for a given Issue on a single iteration.
        """
        query = self.get_issue_query(issue["identifier"],
                                      children_first = children_first, children_after = children_after,
                                      inverse_relations_first = inverse_relations_first, inverse_relations_after = inverse_relations_after,
                                      relations_first = relations_first, relations_after = relations_after,
                                      history_first = history_first, history_after = history_after
                                    )

        data = self.fetch_data(query)
        new_issue = data["issue"]

        # Append the new data to the existing issue
        if len(new_issue["children"]["nodes"]) > 0:
          self.replace_or_append_field(issue, "children", new_issue["children"], ["nodes"])
        if len(new_issue["inverseRelations"]["nodes"]) > 0:
          self.replace_or_append_field(issue, "inverseRelations", new_issue["inverseRelations"], ["nodes"] )
        if len(new_issue["relations"]["nodes"]) > 0:
          self.replace_or_append_field(issue, "relations", new_issue["relations"], ["nodes"])
        if len(new_issue["history"]["nodes"]) > 0:
          self.replace_or_append_field(issue, "history", new_issue["history"], ["nodes"])

        return new_issue

    def get_issue_all_data(self, issueid: str, progress_cb: Optional[Callable[[int, int], None]] = None, ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get all the data for a given Issue.
        """
        issue = {}
        issue["identifier"] = issueid
        children_first = inverse_relations_first = relations_first = history_first = 50
        children_after = inverse_relations_after = relations_after = history_after = None
        while True:
            issue = self.get_issue_all_data_once(issue, ignore_errors = ignore_errors,
                                                children_first = children_first, children_after = children_after,
                                                inverse_relations_first = inverse_relations_first,
                                                inverse_relations_after = inverse_relations_after,
                                                relations_first = relations_first, relations_after = relations_after,
                                                history_first = history_first, history_after = history_after)
            #if progress_cb is not None:
            #    progress_cb(len(issues), data['issue']['totalCount'])
            break
        return issue

