#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import json
import csv
import table
import dirtree

from argparse import ArgumentParser

from vegomatic.gqlf_linear import GqlFetchLinear


def example_fetch_teams(client: GqlFetchLinear) -> list[dict]:
    teams = client.get_teams()
    return teams

def example_fetch_issue(client: GqlFetchLinear, issue: str) -> dict:
    issue = client.get_issue(issue)
    return issue

def example_fetch_issues(client: GqlFetchLinear, team: str) -> list[dict]:
    issues = client.get_issues(team)
    return issues

if __name__ == "__main__":
    parser = ArgumentParser(description='Data Fetch')
    parser.add_argument('--fetch', type=str, choices=['teams', 'issue', 'issues'], help='What to fetch (teams, issue, issues)')
    parser.add_argument('--format', type=str, choices=['json', 'csv', 'table', 'dirtree'], help='Format of the output (json, csv, table, dirtree)')
    parser.add_argument('--team', type=str, default=None, help='Team to fetch from')
    parser.add_argument('--issue', type=str, default=None, help='Issue to fetch')

    args = parser.parse_args()

    # We need a team if we are fetching issues
    if args.fetch == 'issues' and args.team is None:
        parser.error('Team is required for fetching issues')

    client = GqlFetchLinear()
    client.connect()

    retval = None
    if args.fetch == 'teams':
        retval = example_fetch_teams(client)
    elif args.fetch == 'issue':
        retval = example_fetch_issue(client, args.issue)
    elif args.fetch == 'issues':
        retval = example_fetch_issues(client, args.team)

    if args.format == 'json':
        print(json.dumps(retval, indent=4))
    elif args.format == 'csv':
        print(csv.dumps(retval, indent=4))
    elif args.format == 'table':
        print(table.dumps(retval, indent=4))
    elif args.format == 'dirtree':
        print(dirtree.dumps(retval, indent=4))

    print("\n=== All examples completed successfully! ===")