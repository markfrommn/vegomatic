#!/usr/bin/env python3
"""
Simple example demonstrating the GqlFetch module Github module.
"""

from vegomatic.gqlf_github import GqlFetchGithub
from argparse import ArgumentParser
import pprint

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

def print_repo(repository, long_format=False):
    if not long_format:
        print("{} - Created: {}, Updated: {}, Description: {}".format(
            repository['name'], repository['createdAt'], repository['updatedAt'], repository['description']))
    else:
        print("Repository: {}".format(repository['name']))
        print_repo = repository.copy()
        del print_repo['name']
        pprint.pprint(print_repo)

def print_pr(pr, long_format=False):
    if not long_format:
        print("{} - Created: {}, Merged: {}, Closed: {}".format(
            pr['title'], pr['createdAt'], pr['mergedAt'], pr['closedAt']))
    else:
        print("PR: {}".format(pr['title']))
        pprint.pprint(pr)

if __name__ == "__main__":
    fetch_type = None

    parser = ArgumentParser(description='Github Fetch')
    parser.add_argument('--organization', type=str, required=True, help='Organization to fetch')
    parser.add_argument('--long', action='store_true', help='Long format')
    parser.add_argument('--print-query', action='store_true', help='Print the query')
    parser.add_argument('--repository', type=str, help='Repository to fetch')
    args = parser.parse_args()

    if args.repository:
        fetch_type = 'prs'
    else:
        fetch_type = 'repos'

    gh = GqlFetchGithub()

    if args.print_query:
        if fetch_type == 'repos':
            query = gh.get_repository_query(args.organization)        
        elif fetch_type == 'prs':
            query = gh.get_pr_query(args.organization, args.repository)

        else:
            print("Invalid fetch type")
            exit(1)
        print("Query:\n{}".format(query))
        exit(0)

    gh.connect()

    if fetch_type == 'repos':
        repositories = gh.get_repositories(organization=args.organization, progress_cb=progress_cb)
        for repository in repositories:
            print_repo(repository=repository, long_format=args.long)
    elif fetch_type == 'prs':
        prs = gh.get_prs(organization=args.organization, repository=args.repository, progress_cb=progress_cb)
        for pr in prs:
            print_pr(pr=pr, long_format=args.long)
    else:
        print("Invalid fetch type")
        exit(1)
    exit(0)
    
