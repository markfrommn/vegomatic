"""
datafetch - A set of utilities for fetching data from a database.

This module provides a simple interface for database operations using the pydal library.
"""

import sys
from typing import Callable, Tuple, Union

from pydal import DAL

class DataFetch:
    """
    A class to manage database connections and operations using pydal.
    
    Attributes
    ----------
    db : Union[DAL, None]
        The database connection object using pydal's DAL, or None if not connected
    
    Methods
    -------
    clear()
        Closes the database connection and frees resources
    create(dburl: str)
        Creates a new database connection using the provided URL
    """
    
    db: Union[DAL, None] = None

    def __init__(self):
        """
        Initialize a new DataFetch instance.
        
        The database connection is initially set to None and cleared.
        """
        self.db = None
        self.clear()

    def clear(self):
        """
        Clear and close the current database connection.
        
        If a database connection exists, it will be closed and resources will be freed.
        The database connection object will be set to None.
        
        Returns
        -------
        None
        """
        if self.db is not None:
            del self.db  # No real destructor/close but does free a bunch O ram
            self.db = None
        return

    def create(self, dburl: str) -> bool:
        """
        Create a new database connection.
        
        Parameters
        ----------
        dburl : str
            The database URL string in pydal format
            
        Returns
        -------
        bool
            True if the connection was successfully created
            
        Examples
        --------
        >>> df = DataFetch()
        >>> df.create("sqlite://storage.db")
        True
        """
        self.db = DAL(dburl)
        return True
