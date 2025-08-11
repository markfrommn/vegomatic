__version__ = "0.2.0"

# Import main modules
from .gqlfetch import GqlFetch, PageInfo
from .gqlf_github import GqlFetchGithub

__all__ = [
    "GqlFetch",
    "GqlFetchGithub",
    "PageInfo",
]
