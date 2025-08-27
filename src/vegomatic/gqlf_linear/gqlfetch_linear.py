"""
GqlFetch-linear module for fetching data from the Linear GraphQL endpoint with pagination support.
"""

from typing import Any, Callable, Dict, List, Mapping, Optional
import json

from vegomatic.gqlfetch import GqlFetch
class GqlFetchLinear(GqlFetch):
    """
    A GraphQL query for fetching Teams from the Linear.app GraphQL endpoint with pagination support.
    """
    # The base query for teams in a Linear Organization.
    team_query = """
      query Teams {
        teams (<TEAM_ARGS>) {
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

    """
    A GraphQL query for fetching Users from the Linear.app GraphQL endpoint with pagination support.
    """
    # The base query for users in a Linear Organization.
    user_query = """
      query Users {
        users(<USER_ARGS>) {
          nodes {
            id
            name
            email
            active
            app
            gitHubUserId
            displayName
            createdAt
            updatedAt
            lastSeen
            createdIssueCount
            statusLabel
            url
            description
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    """

    #
    # Node portion of group issue query
    #
    issue_query_core = """
          issues (<ISSUES_ARGS>) {
            nodes {
              id
              identifier
              title
              description
              createdAt
              startedAt
              completedAt
              updatedAt
              archivedAt
              url
              activitySummary
              state {
                id
                name
              }
            }
            pageInfo {
                hasNextPage
                endCursor
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
          <ISSUE_QUERY_CORE>
        }
      }
    """

    # Sample TEAM ARGS = id: "ff155ab0-e5b3-4373-9090-0ddde594c034") {
    # We keep this short because we have to refetch by issue anyway
    issue_group_query = """
      query Issues {
          <ISSUE_QUERY_CORE>
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
          title
          description
          createdAt
          startedAt
          completedAt
          updatedAt
          archivedAt
          url
          activitySummary
          state {
            id
            name
          }
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

    @classmethod
    def clean_issue(cls, issue: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Clean a single Issue.
        Delete empty sub-dicts and hasNextPage false from Issues
        """
        if issue.get('children', {}):
          if (len(issue['children']['nodes']) == 0):
            del issue['children']
          elif issue['children'].get('pageInfo', {}).get('hasNextPage') is False:
            del issue['children']['pageInfo']
        if issue.get('inverseRelations', {}):
          if (len(issue['inverseRelations'].get('nodes', [])) == 0):
            del issue['inverseRelations']
          elif issue['inverseRelations'].get('pageInfo', {}).get('hasNextPage') is False:
            del issue['inverseRelations']['pageInfo']
        if issue.get('relations', {}):
          if (len(issue['relations'].get('nodes', [])) == 0):
            del issue['relations']
          elif issue['relations'].get('pageInfo', {}).get('hasNextPage') is False:
            del issue['relations']['pageInfo']
        if issue.get('history', {}):
          if (len(issue['history'].get('nodes', [])) == 0):
            del issue['history']
          elif issue['history'].get('pageInfo', {}).get('hasNextPage') is False:
            del issue['history']['pageInfo']
        return issue

    @classmethod
    def clean_issues(cls, issues: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        """
        Clean a list of Issues.

        Delete empty sub-dicts and hasNextPage false from Issues
        """
        for id in issues:
            issue = cls.clean_issue(issues[id])
            issues[id] = issue
        return issues

    @classmethod
    def nested_get(indict: Mapping, keys: List[str]) -> Any:
      """
      Get a nested property from a dict.
      """
      outvalue = indict
      for key in keys:
          if not isinstance(outvalue, (dict, Mapping)):
              raise TypeError(f"Cannot access key '{key}' - value is not a mapping")
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

    def get_teams_query(self, first: int = 50, after: Optional[str] = None) -> str:
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

    def get_users_query(self, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a query for a users in a Linear Organization.
        """
        user_first_arg = user_after_arg = comma_arg = ""

        if (first is not None):
            user_first_arg = f"first: {first}"
        if (after is not None):
            user_after_arg = f'after: "{after}"'
        if user_first_arg != "" and user_after_arg != "":
            comma_arg = ", "

        user_query_args = f"{user_first_arg}{comma_arg}{user_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.user_query.replace("<USER_ARGS>", user_query_args)
        return query

    def get_team_issues_query(self, team: str, first: int = 50, after: Optional[str] = None) -> str:
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
        query = self.issue_query_by_team.replace("<ISSUE_QUERY_CORE>", self.issue_query_core)
        query = query.replace("<TEAM_ARGS>", query_team_arg)
        query = query.replace("<ISSUES_ARGS>", issue_query_args)
        # print(query)
        return query

    def get_issues_query(self, first: int = 50, after: Optional[str] = None) -> str:
        """
        Get a issues query for a given Team.
        """
        issue_first_arg = issue_after_arg = comma_arg = ""

        if (first is not None):
            issue_first_arg = f"first: {first}"
        if (after is not None):
            issue_after_arg = f'after: "{after}"'
        if issue_first_arg != "" and issue_after_arg != "":
            comma_arg = ", "

        issue_query_args = f"{issue_first_arg}{comma_arg}{issue_after_arg}"
        # We can't use format() here because the query is filled with curly braces
        query = self.issue_group_query.replace("<ISSUE_QUERY_CORE>", self.issue_query_core)
        query = query.replace("<ISSUES_ARGS>", issue_query_args)
        # print(query)
        return query

    def get_issue_all_data_query(self, issue: str,
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
        query = self.get_teams_query(first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_teams(self, first = 50, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        """
        Get a list of teams, iterating over all pages.
        """
        teams = []
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            if limit is not None and limit < (first + len(teams)):
                first = limit - len(teams)
            if first < 1:
                break
            data = self.get_teams_once(first, after, ignore_errors)
            newteams = data.get('teams', {}).get('nodes', [])
            teams.extend(newteams)
            hasmore = data['teams']['pageInfo']['hasNextPage']
            endCursor = data['teams']['pageInfo']['endCursor']
            if not hasmore:
                break
            if limit is not None and len(teams) >= limit:
                break
            after = endCursor
        return teams

    def get_users_once(self, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get a list of users without for one batch.
        """
        query = self.get_users_query(first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_users(self, first = 50, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        """
        Get a list of users, iterating over all pages.
        """
        users = []
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            if limit is not None and limit < (first + len(users)):
                first = limit - len(users)
            if first < 1:
                break
            data = self.get_users_once(first, after, ignore_errors)
            newusers = data.get('users', {}).get('nodes', [])
            users.extend(newusers)
            hasmore = data['users']['pageInfo']['hasNextPage']
            endCursor = data['users']['pageInfo']['endCursor']
            if not hasmore:
                break
            if limit is not None and len(users) >= limit:
                break
            after = endCursor
        return users

    def get_team_issues_once(self, team: str, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> List[Mapping[str, Any]]:
        """
        Get a list of Issues for a team
        """
        query = self.get_team_issues_query(team, first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_team_issues(self, team: str, first = 50, ignore_errors: bool = False, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        """
        Get a list of Issues for a given Team.

        We build this as a dict of issues, keyed by issue id do that we can merge the data from get_issue_all_data() later.
        """
        issues = {}
        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            if limit is not None and limit < (first + len(issues)):
                first = limit - len(issues)
            if first < 1:
                break
            data = self.get_team_issues_once(team, first, after, ignore_errors)
            newissues = data.get('team', {}).get('issues', {}).get('nodes', [])
            for issue in newissues:
                # Flag that we are incomplete so we can replace later instead of having to merge
                issue["is_full"] = False
                issues[issue['identifier']] = issue
            hasmore = data['team']['issues']['pageInfo']['hasNextPage']
            endCursor = data['team']['issues']['pageInfo']['endCursor']
            if not hasmore:
                break
            if limit is not None and len(issues) >= limit:
                break
            after = endCursor
        return issues

    def get_issues_once(self, issues: Mapping[str, Any] = None, first: int = 50, after: Optional[str] = None, ignore_errors: bool = False) -> dict[str, Any]:
        """
        Get a list of Issues for a team
        """
        if issues is None:
          issues = {}
        query = self.get_issues_query(first, after)
        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_issues(self, first = 50, batch_cb: Optional[Callable[[Mapping[str, Any], str], None]] = None, ignore_errors: bool = False, limit: Optional[int] = None) -> Dict[str, Mapping[str, Any]]:
        """
        Get a list of Issues.

        We build this as a dict of issues, keyed by issue id do that we can merge the data from get_issue_all_data() later.

        Note if batch_cb is provided, it will be called with the issues dict and the issue data.  In that case we will not build the issues dict.
        """
        issues = {}

        after = None
        if limit is not None and limit < first:
          first = limit
        while True:
            if limit is not None and limit < (first + len(issues)):
                first = limit - len(issues)
            if first < 1:
                break
            data = self.get_issues_once(issues, first, after, ignore_errors)
            # print(json.dumps(data, indent=4))
            newissues = data.get('issues', {}).get('nodes', [])
            for issue in newissues:
                # Flag that we are incomplete so we can replace later instead of having to merge
                issue["is_full"] = False
                issues[issue['identifier']] = issue
            hasmore = data['issues']['pageInfo']['hasNextPage']
            endCursor = data['issues']['pageInfo']['endCursor']
            if batch_cb is not None:
                batch_cb(newissues, endCursor)
                # Reset the issues dict for the next batch if using the callback and/or so we return an empty dict
                issues = {}
            if not hasmore:
                break
            if limit is not None and len(issues) >= limit:
                break
            after = endCursor
        return issues


    def get_issue_all_data_once(self, issueid: str,
                                children_first: int = 50, children_after: Optional[str] = None,
                                inverse_relations_first: int = 50, inverse_relations_after: Optional[str] = None,
                                relations_first: int = 50, relations_after: Optional[str] = None,
                                history_first: int = 50, history_after: Optional[str] = None,
                                ignore_errors: bool = False) -> dict[str, Any]:
        """
        Get all data for a given Issue on a single iteration.
        """
        query = self.get_issue_all_data_query(issueid,
                                      children_first = children_first, children_after = children_after,
                                      inverse_relations_first = inverse_relations_first, inverse_relations_after = inverse_relations_after,
                                      relations_first = relations_first, relations_after = relations_after,
                                      history_first = history_first, history_after = history_after
                                    )

        data = self.fetch_data(query, ignore_errors=ignore_errors)
        return data

    def get_issue_all_data(self, issueid: str, ignore_errors: bool = False) -> dict[str, dict[str, Any]]:
        """
        Get all the data for a given Issue.
        """
        issue = None
        children_first = inverse_relations_first = relations_first = history_first = 50
        children_after = inverse_relations_after = relations_after = history_after = None
        while True:
            need_more = False
            data = self.get_issue_all_data_once(issueid, ignore_errors = ignore_errors,
                                                children_first = children_first, children_after = children_after,
                                                inverse_relations_first = inverse_relations_first,
                                                inverse_relations_after = inverse_relations_after,
                                                relations_first = relations_first, relations_after = relations_after,
                                                history_first = history_first, history_after = history_after)
            newissue = data["issue"]
            # if it is our first pass, just take the new issue
            if issue is None:
                issue = newissue
            else:
              # Append the new data to the existing issue
              if len(newissue["children"]["nodes"]) > 0:
                  self.replace_or_append_field(issue, "children", newissue["children"], ["nodes"])
              if len(newissue["inverseRelations"]["nodes"]) > 0:
                  self.replace_or_append_field(issue, "inverseRelations", newissue["inverseRelations"], ["nodes"])
              if len(newissue["relations"]["nodes"]) > 0:
                  self.replace_or_append_field(issue, "relations", newissue["relations"], ["nodes"])
              if len(newissue["history"]["nodes"]) > 0:
                  self.replace_or_append_field(issue, "history", newissue["history"], ["nodes"])
            # We have to check for multiple continuations but we can do them all at once
            children_have_more = issue.get('children', {}).get('pageInfo', {}).get('hasNextPage')
            children_after = issue.get('children', {}).get('pageInfo', {}).get('endCursor')
            if children_first > 0 and children_have_more:
              need_more = True
            else:
              children_after = None
              children_first = 0
            inverse_relations_have_more = issue.get('inverseRelations', {}).get('pageInfo', {}).get('hasNextPage')
            inverse_relations_after = issue.get('inverseRelations', {}).get('pageInfo', {}).get('endCursor')
            if inverse_relations_first > 0 and inverse_relations_have_more:
              need_more = True
            else:
              inverse_relations_after = None
              inverse_relations_first = 0
            relations_have_more = issue.get('relations', {}).get('pageInfo', {}).get('hasNextPage')
            relations_after = issue.get('relations', {}).get('pageInfo', {}).get('endCursor')
            if relations_first > 0 and relations_have_more:
              need_more = True
            else:
              relations_after = None
              relations_first = 0
            history_have_more = issue.get('history', {}).get('pageInfo', {}).get('hasNextPage')
            history_after = issue.get('history', {}).get('pageInfo', {}).get('endCursor')
            if history_first > 0 and history_have_more:
              need_more = True
            else:
              history_after = None
              history_first = 0
            if not need_more:
              break
            #if progress_cb is not None:
            #    progress_cb(len(issues), data['issue']['totalCount'])
            break
        return issue

