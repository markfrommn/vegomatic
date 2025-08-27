#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import json
import csv
import sys
import time
#import dirtree
from enum import Enum
from typing import Mapping
from vegomatic.datafile import dictionary_to_json_files
import pandas as pd

from argparse import ArgumentParser

from vegomatic.gqlf_linear import GqlFetchLinear

# argparse helpers
class OutputFormat(Enum):
    """Enumeration of available output formats for data export."""
    LIST = 'list'
    TABLE = 'table'
    JSON = 'json'
    CSV = 'csv'
    DIRTREE = 'dirtree'

    def __str__(self):
        """Return string representation of the enum value."""
        return self.value

    def __repr__(self):
        """Return string representation of the enum value."""
        return self.value

class FetchType(Enum):
    """Enumeration of available fetch types for Linear data."""
    TEAMS = 'teams'
    ISSUES = 'issues'
    ISSUE = 'issue'
    TEAM_ISSUES = 'teamissues'
    ALL_ISSUES = 'allissues'

    def __str__(self):
        """Return string representation of the enum value."""
        return self.value

    def __repr__(self):
        """Return string representation of the enum value."""
        return self.value

# State for fetch all issues
fetch_all_outdir = None
fetch_all_fullissue = False
fetch_all_count = 0
fetch_all_client = None
fetch_all_throttle = 0
fetch_all_max_batch = 1 # Set max batch > 0 to avoid /0 errors and other corner cases

def fetch_issues_callback(issues: Mapping[str, dict], endCursor: str) -> None:
    """
    Callback function for processing batches of issues during pagination.

    Args:
        issues: Dictionary mapping issue IDs to issue data
        endCursor: The cursor position after this batch
    """
    global fetch_all_outdir
    global fetch_all_fullissue
    global fetch_all_count
    global fetch_all_client
    global fetch_all_max_batch

    batch_count = len(issues)
    if batch_count > fetch_all_max_batch:
        fetch_all_max_batch = batch_count
    fetch_all_count += batch_count

    if fetch_all_fullissue:
        status_endl = "\n"
        print(f"Starting {batch_count} for {fetch_all_count} issues to {fetch_all_outdir} at cursor: {endCursor}...", end=status_endl)
    else:
        status_endl = "\r"
    for issueid, issue in issues.items():
        assert issueid == issue['identifier'], f"Issue ID mismatch: {issueid} != {issue['identifier']}"

        if fetch_all_fullissue:
            issue = fetch_all_client.get_issue_all_data(issueid)
            print(".", end="", flush=True)
            # Linear has a rate limit of 1500 requests per hour, so we sleep for 4 seconds per to be safe
            time.sleep(2.7)
        issue = GqlFetchLinear.clean_issue(issue)
        issues[issueid] = issue

    assert fetch_all_outdir is not None, 'fetch_all_outdir is not set'

    dictionary_to_json_files(fetch_all_outdir, issues)
    print(f"...Processed {batch_count} for {fetch_all_count} issues to {fetch_all_outdir} at cursor: {endCursor}...", end=status_endl)
    if fetch_all_throttle is not None and fetch_all_throttle > 0:
        # We need to throttle the requests to avoid rate limiting
        # If we got a small batch adjust the throttle to be a fraction of the batch size with a bit of debouncing
        if batch_count < (fetch_all_max_batch * 0.95):
            adjust_ratio = batch_count / fetch_all_max_batch
        else:
            adjust_ratio = 1.0
        this_sleep = fetch_all_throttle * adjust_ratio
        print(f"...throttling for {this_sleep} seconds.")
        time.sleep(this_sleep)

def example_fetch_teams(linearclient: GqlFetchLinear) -> list[dict]:
    """
    Example function to fetch all teams from Linear.

    Args:
        linearclient: The Linear GraphQL client instance

    Returns:
        list[dict]: List of team dictionaries
    """
    teams = linearclient.get_teams()
    return teams

def example_fetch_issue(linearclient: GqlFetchLinear, issueid: str) -> dict:
    """
    Example function to fetch a single issue from Linear.

    Args:
        linearclient: The Linear GraphQL client instance
        issueid: The ID of the issue to fetch

    Returns:
        dict: Issue data dictionary
    """
    efissue = linearclient.get_issue_all_data(issueid)
    return efissue

def example_fetch_team_issues(linearclient: GqlFetchLinear, team: str, limit: int = None) -> list[dict]:
    """
    Example function to fetch issues for a specific team from Linear.

    Args:
        linearclient: The Linear GraphQL client instance
        team: The team name or ID to fetch issues for
        limit: Optional limit on the number of issues to fetch

    Returns:
        list[dict]: List of issue dictionaries
    """
    efissues = linearclient.get_team_issues(team, limit=limit)
    return efissues

def example_fetch_issues(linearclient: GqlFetchLinear, limit: int = None) -> list[dict]:
    """
    Example function to fetch all issues from Linear.

    Args:
        linearclient: The Linear GraphQL client instance
        limit: Optional limit on the number of issues to fetch

    Returns:
        list[dict]: List of issue dictionaries
    """
    issues = linearclient.get_issues(limit=limit)
    return issues

def example_fetch_all_issues(linearclient: GqlFetchLinear, outdir: str = None, fullissue: bool = False, throttle: float = 30) -> None:
    """
    Example function to fetch all issues from Linear with batch processing.

    Args:
        linearclient: The Linear GraphQL client instance
        outdir: Output directory for saving issue data
        fullissue: Whether to fetch full issue data (requires additional API calls)
        throttle: Throttle time between requests in seconds

    Note:
        This function uses global variables to manage state during batch processing.
        It processes issues in batches and saves them to the specified output directory.
    """
    global fetch_all_outdir
    global fetch_all_client
    global fetch_all_fullissue
    global fetch_all_throttle
    global fetch_all_max_batch

    fetch_all_outdir = outdir
    fetch_all_fullissue = fullissue
    fetch_all_throttle = throttle

    if fullissue:
        # We need a new client for fetching full issue data while the original client is active fetching issues (GraphQL library isn't happy w/recursion)
        fetch_all_client = GqlFetchLinear()
        fetch_all_client.connect()

    linearclient.get_issues(limit=None, batch_cb=fetch_issues_callback)
    return

#
# Main
#
if __name__ == "__main__":
    parser = ArgumentParser(description='Linear Data Fetch')

    # What to fetch
    parser.add_argument('--fetch', type=FetchType, default=FetchType.TEAMS, choices=list(FetchType), help='What to fetch (teams, issue, issues, teamissues, allissues)')
    parser.add_argument('--team', type=str, default=None, help='Team to fetch from')
    parser.add_argument('--issue', type=str, default=None, help='Issue to fetch')
    parser.add_argument('--fullissue', action='store_true', help='Fetch full issue data')

    # Output format
    parser.add_argument('--format', type=OutputFormat, default=OutputFormat.TABLE, choices=list(OutputFormat))
    parser.add_argument('--outfile', type=str, default=None, help='Output file')
    parser.add_argument('--outdir', type=str, help='Output directory (used for dirtree)')

    # Query options
    parser.add_argument('--print-query', action='store_true', help='Print the query')
    parser.add_argument('--ignore-errors', action='store_true', help='Ignore errors')
    parser.add_argument('--limit', type=int, default=100, help='Stop when at least this many items have been fetched')
    parser.add_argument('--throttle', type=float, default=0, help='Throttle between requests in seconds')

    args = parser.parse_args()

    # We need a team if we are fetching teamissues
    if args.fetch == FetchType.TEAM_ISSUES and args.team is None:
        parser.error('Team is required for fetching team issues')
        sys.exit(1)

    if args.fetch == FetchType.ALL_ISSUES:
        if args.format not in [OutputFormat.JSON, OutputFormat.DIRTREE]:
            parser.error('Cannot use --allissues with format other than json or dirtree')
            sys.exit(1)

    if args.issue is not None:
        args.fetch = 'issue'

    client = GqlFetchLinear()
    client.connect()

    columns = None
    issue_columns = [ 'key', 'identifier', 'createdAt', 'startedAt', 'completedAt', 'title' ] #  'id', 'description', 'url']
    retval = None

    if args.fetch == FetchType.ALL_ISSUES:
        if args.outdir is None:
            parser.error('Cannot use --allissues without --outdir')
            sys.exit(1)
        example_fetch_all_issues(client, args.outdir, args.fullissue)
        sys.exit(0)
    elif args.fetch == FetchType.TEAMS:
        retval = example_fetch_teams(client)
    elif args.fetch == FetchType.ISSUE:
        issue = example_fetch_issue(client, args.issue)
        retval = [ issue ]
        columns = issue_columns
    elif args.fetch == FetchType.TEAM_ISSUES:
        retval1 = example_fetch_team_issues(client, args.team, args.limit)
        retval = list(retval1.values())
        columns = issue_columns
    elif args.fetch == FetchType.ISSUES:
        retval1 = example_fetch_issues(client, args.limit)
        retval2 = GqlFetchLinear.clean_issues(retval1)
        retval = list(retval2.values())
        columns = issue_columns
    else:
        parser.error(f"Unknown fetch type: {args.fetch}")
        sys.exit(1)

    if args.fullissue:
        fullissues = {}
        for issue in retval:
            newissue = client.get_issue_all_data(issue['identifier'])
            newissue = GqlFetchLinear.clean_issue(newissue)
            fullissues[issue['identifier']] = newissue
        retval = fullissues

    # retval may be be a dict - we need to convert to a list of values unless output is JSON or dirtree
    if isinstance(retval, dict) and args.format not in [OutputFormat.JSON, OutputFormat.DIRTREE]:
        retval = list(retval.values())

    if args.format == OutputFormat.CSV or args.format == OutputFormat.TABLE:
        df = pd.DataFrame(retval)
        if columns is not None:
            # Slice out all rows plus the columns we want
            df = df.loc[:, df.columns.isin(columns)]

    if args.format == OutputFormat.JSON:
        print(json.dumps(retval, indent=4))
    elif args.format == OutputFormat.CSV:
        if args.outfile is None:
            args.outfile = "/dev/stdout"
        df.to_csv(args.outfile, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    elif args.format == OutputFormat.TABLE or args.format == OutputFormat.LIST:
        print(df.to_string())
    elif args.format == OutputFormat.DIRTREE:
        dictionary_to_json_files(args.outdir, retval)
    else:
        print("Unknown format: ", args.format)

    print("\n=== All examples completed successfully! ===")