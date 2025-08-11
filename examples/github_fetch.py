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

from vegomatic.gqlf_github import GqlFetchGithub

class OutputFormat(Enum):
    LONG = 'long'
    SHORT = 'short'
    JSON = 'json'
    CSV = 'csv'

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


def pretty_print(clas, indent=0):
    print(' ' * indent +  type(clas).__name__ +  ':')
    indent += 4
    for k,v in clas.__dict__.items():
        if '__dict__' in dir(v):
            pretty_print(v,indent)
        else:
            print(' ' * indent +  k + ': ' + str(v))

def progress_cb(current, total):
    print(f"Progress: {current} of {total}...", end='\n')

def print_repo(repository, format: OutputFormat = OutputFormat.SHORT, outfile=sys.stdout):
    if format == OutputFormat.SHORT:
        print("{} - Created: {}, Updated: {}, Description: {}".format(
            repository['name'], repository['createdAt'], repository['updatedAt'], repository['description']), file=outfile)
    else:
        print("Repository: {}".format(repository['name']), file=outfile)
        print_repo = repository.copy()
        del print_repo['name']
        pprint.pprint(print_repo, stream=outfile)
    outfile.flush()

def print_pr(pr, format: OutputFormat = OutputFormat.SHORT, outfile=sys.stdout):
    if format == OutputFormat.SHORT:
        print("{} - State: {}, Created: {}, Merged: {}, Closed: {}, Title: {}".format(
            pr['number'], pr['state'], pr['createdAt'], pr['mergedAt'], pr['closedAt'], pr['title']), file=outfile)
    else:
        print("PR: {}".format(pr['title']), file=outfile)
        pprint.pprint(pr, stream=outfile)
    outfile.flush()

if __name__ == "__main__":
    fetch_type = None
    outfile = None

    parser = ArgumentParser(description='Github Fetch')
    parser.add_argument('--organization', type=str, required=True, help='Organization to fetch')
    parser.add_argument('--format', type=OutputFormat, default=OutputFormat.SHORT, choices=list(OutputFormat))
    parser.add_argument('--print-query', action='store_true', help='Print the query')
    parser.add_argument('--repository', type=str, help='Repository to fetch')
    parser.add_argument('--outbase', type=str, help='Output file base name')
    parser.add_argument('--ignore-errors', action='store_true', help='Ignore errors')
    parser.add_argument('--limit', type=int, default=100, help='Stop when at least this many items have been fetched')
    args = parser.parse_args()

    if args.repository:
        fetch_type = 'prs'
    else:
        fetch_type = 'repos'

    gh = GqlFetchGithub()

    if args.print_query:
        if fetch_type == 'repos':
            query = gh.get_repository_query(organization=args.organization, ignore_errors=True)        
        elif fetch_type == 'prs':
            query = gh.get_pr_query(organization=args.organization, repository=args.repository, ignore_errors=True)

        else:
            print("Invalid fetch type")
            exit(1)
        print("Query:\n{}".format(query))
        exit(0)

    if args.outbase:
        outname = args.outbase + '.' + args.format.value
        outfile = open(outname, 'w')
    else:
        outfile = sys.stdout

    gh.connect()

    if fetch_type == 'repos':
        repositories = gh.get_repositories(organization=args.organization, progress_cb=progress_cb, ignore_errors=args.ignore_errors, limit=args.limit)
        if args.format == OutputFormat.SHORT or args.format == OutputFormat.LONG:
            for repository in repositories:
                print_repo(repository=repository, format=args.format, outfile=outfile)
        elif args.format == OutputFormat.JSON:
            json.dump(repositories, outfile)
        elif args.format == OutputFormat.CSV:
            cw = csv.DictWriter(outfile, fieldnames=repositories[0].keys(), dialect='unix')
            cw.writeheader()
            cw.writerows(repositories)
    elif fetch_type == 'prs':
        prs = gh.get_prs(organization=args.organization, repository=args.repository, progress_cb=progress_cb, ignore_errors=args.ignore_errors, limit=args.limit)
        prs = gh.clean_prs(prs, clean_all=(args.format == OutputFormat.CSV))
        if args.format == OutputFormat.SHORT or args.format == OutputFormat.LONG:
            for pr in prs:
                print_pr(pr=pr, format=args.format, outfile=outfile)
        elif args.format == OutputFormat.JSON:
            json.dump(prs, outfile)
        elif args.format == OutputFormat.CSV:
            cw = csv.DictWriter(outfile, fieldnames=prs[0].keys(), dialect='unix')
            cw.writeheader()
            cw.writerows(prs)
    else:
        print("Invalid fetch type")
        exit(1)

    if outfile is not None:
        outfile.close()
    exit(0)
    
