#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import json
import csv
import sys
import time
#import dirtree
from typing import Mapping
from vegomatic.datafile import dictionary_to_json_files
import pandas as pd

from argparse import ArgumentParser

from vegomatic.gqlf_linear import GqlFetchLinear

fetch_all_outdir = None
fetch_all_fullissue = False
fetch_all_count = 0
fetch_all_client = None

def fetch_issues_callback(issues: Mapping[str, dict], endCursor: str) -> None:
    """
    Callback for fetching issues.
    """
    global fetch_all_outdir
    global fetch_all_fullissue
    global fetch_all_count
    batch_count = len(issues)
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

def example_fetch_teams(client: GqlFetchLinear) -> list[dict]:
    teams = client.get_teams()
    return teams

def example_fetch_issue(client: GqlFetchLinear, issueid: str) -> dict:
    issue = client.get_issue_all_data(issueid)
    return issue

def example_fetch_team_issues(client: GqlFetchLinear, team: str, limit: int = None) -> list[dict]:
    issues = client.get_team_issues(team, limit=limit)
    return issues

def example_fetch_issues(client: GqlFetchLinear, limit: int = None) -> list[dict]:
    issues = client.get_issues(limit=limit)
    return issues

def example_fetch_all_issues(client: GqlFetchLinear, outdir: str = None, fullissue: bool = False) -> list[dict]:
    global fetch_all_outdir
    global fetch_all_client
    global fetch_all_fullissue
    fetch_all_outdir = outdir
    fetch_all_fullissue = fullissue

    if fullissue:
        # We need a new client for fetching full issue data while the original client is active fetching issues
        fetch_all_client = GqlFetchLinear()
        fetch_all_client.connect()

    client.get_issues(limit=None, batch_cb=fetch_issues_callback)
    return

if __name__ == "__main__":
    parser = ArgumentParser(description='Data Fetch')
    parser.add_argument('--fetch', type=str, default='teams', choices=['teams', 'issue', 'issues', 'teamissues', 'allissues'], help='What to fetch (teams, issue, issues, teamissues, allissues)')
    parser.add_argument('--format', type=str, default='table', choices=['json', 'csv', 'table', 'dirtree'], help='Format of the output (json, csv, table, dirtree)')
    parser.add_argument('--team', type=str, default=None, help='Team to fetch from')
    parser.add_argument('--issue', type=str, default=None, help='Issue to fetch')
    parser.add_argument('--output', type=str, default=None, help='Output file')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of results')
    parser.add_argument('--fullissue', action='store_true', help='Fetch full issue data')
    parser.add_argument('--outdir', type=str, help='Save issues to a directory as JSON files per-issue')

    args = parser.parse_args()

    # We need a team if we are fetching teamissues
    if args.fetch == 'teamissues' and args.team is None:
        parser.error('Team is required for fetching team issues')
        sys.exit(1)

    if args.fetch == 'allissues':
        if args.format not in ['json', 'dirtree']:
            parser.error('Cannot use --allissues with format other than json or dirtree')
            sys.exit(1)

    if args.issue is not None:
        args.fetch = 'issue'

    client = GqlFetchLinear()
    client.connect()

    columns = None
    issue_columns = [ 'key', 'identifier', 'createdAt', 'startedAt', 'completedAt', 'title' ] #  'id', 'description', 'url']
    retval = None

    if args.fetch == 'allissues':
        if args.outdir is None:
            parser.error('Cannot use --allissues without --outdir')
            sys.exit(1)
        example_fetch_all_issues(client, args.outdir, args.fullissue)
        sys.exit(0)
    elif args.fetch == 'teams':
        retval = example_fetch_teams(client)
    elif args.fetch == 'issue':
        issue = example_fetch_issue(client, args.issue)
        retval = [ issue ]
        columns = issue_columns
    elif args.fetch == 'teamissues':
        retval1 = example_fetch_team_issues(client, args.team, args.limit)
        retval = list(retval1.values())
        columns = issue_columns
    elif args.fetch == 'issues':
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

    if args.format == 'csv' or args.format == 'table':
        df = pd.DataFrame(retval)
        if columns is not None:
            # Slice out all rows plus the columns we want
            df = df.loc[:, df.columns.isin(columns)]

    if args.format == 'json':
        print(json.dumps(retval, indent=4))
    elif args.format == 'csv':
        if args.output is None:
            args.output = "/dev/stdout"
        df.to_csv(args.output, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    elif args.format == 'table':
        print(df.to_string())
    elif args.format == 'dirtree':
        dictionary_to_json_files(args.outdir, retval)
    else:
        print("Unknown format: ", args.format)

    print("\n=== All examples completed successfully! ===")