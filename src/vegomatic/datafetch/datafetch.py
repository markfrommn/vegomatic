"""
datafetch - A set of utilities for fetching data from a database.

This module provides a simple interface for database operations using the pydal library.
"""

import sys
from typing import Callable, Tuple, Union, Dict, Any
from dateutil.parser import parse
from datetime import datetime

from pydal import DAL, Field

# Local utility function:

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False
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
    dict_create_table(table_name: str, schema: Dict[str, str])
        Creates a table using dictionary schema with pydal Field objects
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

    def dict_create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """
        Create a table using dictionary schema with pydal Field objects.
        
        Parameters
        ----------
        table_name : str
            The name of the table to create
        schema : Dict[str, str]
            Dictionary mapping column names to data types.
            Supported types: 'string', 'number', 'datetime'
            
        Returns
        -------
        bool
            True if the table was successfully created
            
        Examples
        --------
        >>> df = DataFetch()
        >>> df.create("sqlite://storage.db")
        >>> schema = {
        ...     'id': 'number',
        ...     'name': 'string',
        ...     'created_at': 'datetime'
        ... }
        >>> df.dict_create_table('users', schema)
        True
        """
        if self.db is None:
            raise RuntimeError("No database connection. Call create() first.")
        
        # Map data types to pydal Field types
        type_mapping = {
            'string': 'string',
            'number': 'integer',
            'datetime': 'datetime'
        }
        
        # Create Field objects for each column
        fields = {}
        for column_name, data_type in schema.items():
            if data_type not in type_mapping:
                raise ValueError(f"Unsupported data type: {data_type}. Supported types: {list(type_mapping.keys())}")
            
            field_type = type_mapping[data_type]
            fields[column_name] = Field(column_name, field_type)
        
        # Create the table using pydal
        self.db.define_table(table_name, *fields.values())
        return True

    @classmethod
    def dict_fields(cls, data_list: list) -> list:
        """
        Analyze a list of dictionaries and return pydal Field objects.
        
        For every unique key found in the dictionaries, returns a Field with the key as the name.
        The field type is derived using heuristics based on the values.
        
        Parameters
        ----------
        data_list : list
            List of dictionaries to analyze
            
        Returns
        -------
        list
            List of pydal Field objects
            
        Examples
        --------
        >>> data = [
        ...     {'id': 1, 'name': 'John', 'age': 25.5, 'active': True, 'created': '2023-01-01'},
        ...     {'id': 2, 'name': 'Jane', 'age': 30, 'active': False, 'created': '2023-01-02'}
        ... ]
        >>> fields = DataFetch.dict_fields(data)
        >>> [field.name for field in fields]
        ['id', 'name', 'age', 'active', 'created']
        """
        if not data_list:
            return []
        
        # Collect all unique keys from all dictionaries
        all_keys = set()
        for data_dict in data_list:
            if isinstance(data_dict, dict):
                all_keys.update(data_dict.keys())
        
        fields = []
        for key in sorted(all_keys):
            field_type = cls._infer_field_type(data_list, key)
            fields.append(Field(key, field_type))
        
        return fields
    
    @classmethod
    def _infer_field_type(cls, data_list: list, key: str) -> str:
        """
        Infer the field type for a given key based on the values in the data list.
        
        Parameters
        ----------
        data_list : list
            List of dictionaries to analyze
        key : str
            The key to analyze
            
        Returns
        -------
        str
            The inferred field type for pydal
        """
        values = []
        for data_dict in data_list:
            if isinstance(data_dict, dict) and key in data_dict:
                values.append(data_dict[key])
        
        if not values:
            return 'string'  # Default to string if no values found
        
        # Check for datetime type
        datetime_count = 0
        for value in values:
            if isinstance(value, datetime):
                datetime_count += 1
            elif isinstance(value, str) and is_date(value):
                datetime_count += 1
        
        if datetime_count > 0:
            return 'datetime'
        
        # Check for boolean type
        bool_count = 0
        for value in values:
            if isinstance(value, bool):
                bool_count += 1
            elif isinstance(value, int) and value in (0, 1):
                bool_count += 1
            elif isinstance(value, str) and value.lower() in ('true', 'false'):
                bool_count += 1
        
        if bool_count > 0:
            return 'boolean'
        
        # Check for float type
        float_count = 0
        for value in values:
            if isinstance(value, float):
                float_count += 1
            elif isinstance(value, str):
                try:
                    float(value)
                    float_count += 1
                except ValueError:
                    pass
        
        if float_count > 0:
            return 'double'
        
        # Check for integer type
        int_count = 0
        for value in values:
            if isinstance(value, int):
                int_count += 1
            elif isinstance(value, str):
                try:
                    int(value)
                    int_count += 1
                except ValueError:
                    pass
        
        if int_count > 0:
            return 'integer'
        
        # Default to string for everything else
        return 'string'
