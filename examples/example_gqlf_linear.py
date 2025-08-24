#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module with a real GraphQL endpoint.
"""

import json
import csv
import sys
#import dirtree
import pandas as pd

from argparse import ArgumentParser

from vegomatic.gqlf_linear import GqlFetchLinear


def example_fetch_teams(client: GqlFetchLinear) -> list[dict]:
    teams = client.get_teams()
    return teams

def example_fetch_issue(client: GqlFetchLinear, issue: str) -> dict:
    issue = client.get_issue(issue)
    return issue

def example_fetch_issues(client: GqlFetchLinear, team: str, limit: int = None) -> list[dict]:
    issues = client.get_issues(team, limit=limit)
    return issues

if __name__ == "__main__":
    parser = ArgumentParser(description='Data Fetch')
    parser.add_argument('--fetch', type=str, default='teams', choices=['teams', 'issue', 'issues'], help='What to fetch (teams, issue, issues)')
    parser.add_argument('--format', type=str, default='table', choices=['json', 'csv', 'table', 'dirtree'], help='Format of the output (json, csv, table, dirtree)')
    parser.add_argument('--team', type=str, default=None, help='Team to fetch from')
    parser.add_argument('--issue', type=str, default=None, help='Issue to fetch')
    parser.add_argument('--output', type=str, default=None, help='Output file')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of results')

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
        retval = example_fetch_issues(client, args.team, args.limit)
        retval = GqlFetchLinear.clean_issues(retval)

    if args.format == 'json':
        print(json.dumps(retval, indent=4))
    elif args.format == 'csv':
        if args.output is None:
            csvfile = sys.stdout
        else:
            csvfile = open(args.output, 'w', newline='', encoding='utf-8')
        with csvfile:
            mywriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            mywriter.writerow(retval.keys())
            for row in retval:
                mywriter.writerow(row.values())
    elif args.format == 'table':
        df = pd.DataFrame(retval)
        print(df.to_string())
#    elif args.format == 'dirtree':
#        print(dirtree.dumps(retval, indent=4))
    else:
        print("Unknown format: ", args.format)

    print("\n=== All examples completed successfully! ===")