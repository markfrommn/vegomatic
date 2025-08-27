#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module Github module.
"""

from argparse import ArgumentParser
import csv
from enum import Enum
import json
import pprint
import sys
import os
from typing import Mapping, List
import pandas as pd
import time

from vegomatic.gqlf_github import GqlFetchGithub
from vegomatic.datafile import dictionary_to_json_files

# State for fetch all PRs
fetch_all_outdir = None
fetch_all_count = 0
fetch_all_client = None
fetch_all_throttle = None
fetch_all_max_batch = 1 # Set max batch > 0 to avoid /0 errors and other corner cases

#
# Argparse helpers
#
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
    """Enumeration of available fetch types for GitHub data."""
    REPOS = 'repos'
    REPO_PRS = 'repoprs'
    ALL_PRS = 'allprs'
    PR = 'pr'
    MEMBERS = 'members'

    def __str__(self):
        """Return string representation of the enum value."""
        return self.value

    def __repr__(self):
        """Return string representation of the enum value."""
        return self.value

#
# Pretty print helper - Not used but saved for posterity
#
def pretty_print(clas, indent=0):
    """
    Print a class object in a formatted, indented structure.

    Args:
        clas: The class object to print
        indent: Current indentation level (default: 0)
    """
    print(' ' * indent + type(clas).__name__ + ':')
    indent += 4
    for k, v in clas.__dict__.items():
        if '__dict__' in dir(v):
            pretty_print(v, indent)
        else:
            print(' ' * indent + k + ': ' + str(v))

def fetch_prs_callback(prbatch: List[Mapping], prorg: str, prrepo: str, endCursor: str) -> None:
    """
    Callback function for processing batches of pull requests during pagination.

    Args:
        prbatch: Dictionary mapping PR names to PR data
        prorg: GitHub organization name
        prrepo: GitHub repository name
        endCursor: The cursor position after this batch
    """
    global fetch_all_outdir
    global fetch_all_count
    global fetch_all_throttle
    global fetch_all_max_batch

    batch_count = len(prbatch)
    prdict = {}
    # Cheat and derive our baseline batch size from the largest batch we have seen so far
    if batch_count > fetch_all_max_batch:
        fetch_all_max_batch = batch_count
    fetch_all_count += batch_count

    status_endl = "\r"
    for pr in prbatch:
        pr = GqlFetchGithub.clean_pr(pr)
        prdict[pr['pr_id']] = pr

    # We need an outdir to save the PRs
    assert fetch_all_outdir is not None, 'fetch_all_outdir is not set'

    real_outdir = os.path.join(fetch_all_outdir, prrepo)

    dictionary_to_json_files(real_outdir, prdict)
    print(f"...Processed {batch_count} for {fetch_all_count} PRs to {real_outdir} at cursor: {endCursor}...", end=status_endl)
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

def github_fetch_all_prs(ghclient: GqlFetchGithub, prorg: str, outdir: str = None, throttle: float = 0) -> list[dict]:
    """
    Fetch all pull requests for a GitHub organization with batch processing.

    Args:
        ghclient: The GitHub GraphQL client instance
        prorg: GitHub organization name
        outdir: Output directory for saving PR data
        throttle: Throttle time between requests in seconds

    Returns:
        list[dict]: List of pull request dictionaries

    Note:
        This function uses global variables to manage state during batch processing.
        It processes PRs in batches and saves them to the specified output directory.
    """
    global fetch_all_outdir
    global fetch_all_client
    global fetch_all_throttle

    fetch_all_outdir = outdir
    fetch_all_client = ghclient
    fetch_all_throttle = throttle

    # Github PRs only exist in repository scope, so we have to iterate all repos and fetch all PRs per repo
    repos = ghclient.get_repositories(organization=prorg)
    for reponame, repo in repos.items():
        assert reponame == repo['name'], f"Repository name mismatch: {reponame} != {repo['name']}"
        print(f"Fetching PRs for {prorg}/{reponame} (w/throttle={throttle}):")
        ghclient.get_repo_prs(organization=prorg, repository=reponame, limit=None, batch_cb=fetch_prs_callback)
    return

#
# Main
#
if __name__ == "__main__":
    parser = ArgumentParser(description='Github Data Fetch')
    # What to fetch
    parser.add_argument('--fetch', type=FetchType, default=FetchType.REPOS, choices=list(FetchType), help='What to fetch (repos, repoprs, allprs, pr, members)')
    parser.add_argument('--pr', type=str, default=None, help='SinglePR to fetch')
    parser.add_argument('--organization', type=str, required=True, help='Organization to fetch from')
    parser.add_argument('--repository', type=str, help='Repository to fetch')

    # Output format
    parser.add_argument('--format', type=OutputFormat, default=OutputFormat.TABLE, choices=list(OutputFormat))
    parser.add_argument('--outfile', type=str, default=None, help='Output file')
    parser.add_argument('--outdir', type=str, help='Save to a directory (used for dirtree)')

    # Query options
    parser.add_argument('--print-query', action='store_true', help='Print the query')
    parser.add_argument('--ignore-errors', action='store_true', help='Ignore errors')
    parser.add_argument('--limit', type=int, default=None, help='Stop when at least this many items have been fetched')

    # Misc options
    parser.add_argument('--throttle', type=float, default=3.0, help='Per-batch throttle in seconds')

    args = parser.parse_args()

    # We need a team if we are fetching teamissues
    if args.fetch == FetchType.REPO_PRS and args.repository is None:
        parser.error('Repository is required for fetching repository PRs')
        sys.exit(1)

    if args.fetch == FetchType.ALL_PRS:
        if args.format not in [OutputFormat.JSON, OutputFormat.DIRTREE]:
            parser.error('Cannot use --allprs with format other than json or dirtree')
            sys.exit(1)

    if args.pr is not None:
        args.fetch = FetchType.PR

    if args.fetch == FetchType.PR:
        parser.error('Cannot fetch a single PR - Not implemented')
        sys.exit(1)

    if args.format == OutputFormat.DIRTREE:
        if args.outdir is None:
            if args.fetch == FetchType.REPOS:
                args.outdir = "repos"
            elif args.fetch == FetchType.REPO_PRS:
                args.outdir = "prs-" + args.repository
            elif args.fetch == FetchType.ALL_PRS:
                args.outdir = "allprs"
            elif args.fetch == FetchType.PR:
                args.outdir = "pr"
            elif args.fetch == FetchType.MEMBERS:
                args.outdir = "members"
            else:
                parser.error(f"Cannot use --dirtree with Unknown type {args.fetch}")
                sys.exit(1)
            print(f"Using default outdir: {args.outdir}")

    client = GqlFetchGithub()

    first = 50
    if args.print_query:
        if args.limit is not None:
            if args.limit < first:
                first = args.limit
        if args.fetch == FetchType.REPOS:
            query = client.get_org_repository_query(organization=args.organization, first=first)
        elif args.fetch == FetchType.REPO_PRS:
            query = client.get_pr_query(organization=args.organization, repository=args.repository, first=first)
        elif args.fetch == FetchType.ALL_PRS:
            parser.error('Cannot print query for all PRs - Not implemented')
            sys.exit()
        elif args.fetch == FetchType.MEMBERS:
            query = client.get_org_members_query(organization=args.organization, first=first)
        elif args.fetch == FetchType.PR:
            parser.error('Cannot print query for a single PR - Not implemented')
            sys.exit(1)
        else:
            print("Invalid fetch type")
            exit(1)
        print("Query:\n{}".format(query))
        exit(0)

    if args.outfile:
        outname = args.outfile
        outfile = open(outname, 'w', encoding='utf-8')
    else:
        outfile = sys.stdout

    client.connect()

    columns = None
    repo_columns = [ 'name', 'createdAt', 'updatedAt', 'description' ]
    pr_columns = [ 'number', 'state', 'createdAt', 'mergedAt', 'closedAt', 'title' ]
    member_columns = [ 'name', 'login', 'createdAt', 'databaseId' ]
    retval = None

    if args.fetch == FetchType.ALL_PRS:
        if args.outdir is None:
            parser.error('Cannot use --allprs without --outdir')
            sys.exit(1)
        github_fetch_all_prs(client, args.organization, args.outdir, args.throttle)
        sys.exit(0)
    elif args.fetch == FetchType.MEMBERS:
        retval = client.get_org_members(organization=args.organization, ignore_errors=args.ignore_errors, limit=args.limit)
        columns = member_columns
    elif args.fetch == FetchType.REPOS:
        retval = client.get_repositories(organization=args.organization, ignore_errors=args.ignore_errors, limit=args.limit)
        columns = repo_columns
    elif args.fetch == FetchType.REPO_PRS:
        # Use the callback if we are using dirtree to get progress
        if args.format == OutputFormat.DIRTREE:
            fetch_all_outdir = args.outdir
            fetch_all_throttle = args.throttle
            batch_cb = fetch_prs_callback
        else:
            batch_cb = None
        retval = client.get_repo_prs(organization=args.organization, repository=args.repository, batch_cb=batch_cb, ignore_errors=args.ignore_errors, limit=args.limit)
        columns = pr_columns
    elif args.fetch == FetchType.PR:
        parser.error('Cannot fetch a single PR - Not yet implemented')
        sys.exit(1)
    else:
        parser.error(f"Unknown fetch type: {args.fetch}")
        sys.exit(1)

    # retval may be be a dict - we need to convert to a list of values unless output is JSON or dirtree
    if isinstance(retval, dict) and args.format not in [OutputFormat.JSON, OutputFormat.DIRTREE]:
        retval = list(retval.values())

    if args.format == OutputFormat.CSV or args.format == OutputFormat.TABLE:
        df = pd.DataFrame(retval, columns=columns)
        if columns is not None:
            # Slice out all rows plus the columns we want
            df = df.loc[:, df.columns.isin(columns)]

    if args.format == OutputFormat.JSON:
        print(json.dumps(retval, indent=4), file=outfile)
    elif args.format == OutputFormat.CSV:
        if args.outfile is None:
            args.outfile = "/dev/stdout"
        df.to_csv(args.outfile, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    elif args.format == OutputFormat.TABLE or args.format == OutputFormat.LIST:
        print(df.to_string(),file=outfile)
    elif args.format == OutputFormat.DIRTREE:
        dictionary_to_json_files(args.outdir, retval)
    else:
        print("Unknown format: ", args.format)

    print("\n=== All examples completed successfully! ===")

    if outfile is not None:
        outfile.close()
    sys.exit(0)