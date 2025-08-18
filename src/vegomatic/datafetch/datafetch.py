"""
datafetch - A set of utilities for fetching data from a database.

This module provides a simple interface for database operations using the pydal library.
"""

from typing import Callable, List, Tuple, Union, Dict, Any
from datetime import datetime
from dateutil.parser import parse
from pydal import DAL, Field
# pydal buries Table in the pydal.objects module
from pydal.objects import Table

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
    # normally ValueError is raised, but some formats (like an email) raise ParserError
    except ValueError as ve:
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

    def get_table(self, tablename: str) -> Table | None:
        """
        Get a table from the database by name (oddly pydal does not support this idiom)

        Parameters
        ----------
        tablename : str
            The name of the table to get

        Returns
        -------
        Table | None
            The table object if found, None otherwise
        """
        if self.db is None:
            raise RuntimeError("No database connection. Call create() first.")
        if tablename not in self.db.tables:
            return None
        thetable = getattr(self.db, tablename)
        return thetable

<<<<<<< HEAD
    def dict_create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
=======
    def create_table(self, table_name: str, schema: list[Field]) -> bool:
>>>>>>> 959cdc0 (- Add pydal table creation helpers.)
        """
        Create a table using dictionary schema with pydal Field objects.

        Parameters
        ----------
        table_name : str
            The name of the table to create
        schema : List[Field]
            List of pydal Field objects

        Returns
        -------
        bool
            True if the table was successfully created

        Examples
        --------
        >>> df = DataFetch()
        >>> df.create("sqlite://storage.db")
        >>> schema = [
        ...     Field('id', 'number'),
        ...     Field('name', 'string'),
        ...     Field('created_at', 'datetime')
        ... ]
        >>> df.dict_create_table('users', schema)
        True
        """
        if self.db is None:
            raise RuntimeError("No database connection. Call create() first.")

<<<<<<< HEAD

        # Create the table using pydal
        self.db.define_table(table_name, fields=schema, migrate=True)
        self.db.commit()
        return True

    @classmethod
    def fields_from_dicts(cls, data_list: list[dict], unique_fields: list[str] = None, notnull_fields: list[str] = None) -> list[Field]:
        """
        Analyze a list of dictionaries and return pydal Field objects.

        For every unique key found in the dictionaries, returns a Field with the key as the name.
        The field type is derived using heuristics based on the values.
        """

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

=======
>>>>>>> 959cdc0 (- Add pydal table creation helpers.)
        # Create the table using pydal
        self.db.define_table(table_name, fields=schema, migrate=True)
        self.db.commit()
        return True

    @classmethod
    def fields_from_dicts(cls, data_list: list[dict], unique_fields: list[str] = None, notnull_fields: list[str] = None) -> list[Field]:
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

        # Save first dictionary
<<<<<<< HEAD

        #first_dict = data_list[0]

        # Collect all unique keys from all dictionaries
        all_keys = set()
        for row in data_list:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        fields = []
        skip_fields = set()
        field_types = {}
        for key in sorted(all_keys):
            field_types[key] = None

        # Infer the field types for each key
        for row in data_list:
            for key in all_keys:
                # Skip keys that are not in the current dictionary
                if key not in row.keys():
                    continue
                # if we have not yet inferred the field type, use the new type
                new_field_type = cls._infer_field_type(row[key])
                # False means the value is something we can't handle so skip (object, sub-dict, List, etc.)
                if new_field_type is False:
                    skip_fields.add(key)
                    continue
                if field_types[key] is None:
                    field_types[key] = new_field_type
                # if we have already inferred the field type, check if it is consistent
                elif field_types[key] != new_field_type:
                    raise ValueError(f"Inconsistent field types for key {key}: {field_types[key]} != {new_field_type}")

        # Now remove the fields that we can't handle
        for key in skip_fields:
            all_keys.remove(key)


        # Now create the fields
        for key in all_keys:
            if unique_fields is not None and key in unique_fields:
                field_unique = True
            else:
                field_unique = False
            if notnull_fields is not None and key in notnull_fields:
                field_notnull = True
            else:
                field_notnull = False
            fields.append(Field(key, type=field_types[key], unique=field_unique, notnull=field_notnull))

        # Skip for now - we won't have the original order at this point anyway
        #fields = [field for key in first_dict.keys() for field in fields if field.name == key]
        if len(fields) == 0:
            return None
        return fields

    @classmethod
    def _infer_field_type(cls, value) -> str:
        """
        Infer the field type for a given value.

        Parameters
        ----------
        value : any
            The value to infer the field type for
        """

        first_dict = data_list[0]
=======
        #first_dict = data_list[0]
>>>>>>> 959cdc0 (- Add pydal table creation helpers.)

        # Collect all unique keys from all dictionaries
        all_keys = set()
        for row in data_list:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        fields = []
        skip_fields = set()
        field_types = {}
        for key in sorted(all_keys):
            field_type = cls._infer_field_type(data_list, key)
            fields.append(Field(key, field_type))
            field_types[key] = None

        # Infer the field types for each key
        for row in data_list:
            for key in all_keys:
                # Skip keys that are not in the current dictionary
                if key not in row.keys():
                    continue
                # if we have not yet inferred the field type, use the new type
                new_field_type = cls._infer_field_type(row[key])
                # False means the value is something we can't handle so skip (object, sub-dict, List, etc.)
                if new_field_type is False:
                    skip_fields.add(key)
                    continue
                if field_types[key] is None:
                    field_types[key] = new_field_type
                # if we have already inferred the field type, check if it is consistent
                elif field_types[key] != new_field_type:
                    raise ValueError(f"Inconsistent field types for key {key}: {field_types[key]} != {new_field_type}")

        # Now remove the fields that we can't handle
        for key in skip_fields:
            all_keys.remove(key)


        # Now create the fields
        for key in all_keys:
            if unique_fields is not None and key in unique_fields:
                field_unique = True
            else:
                field_unique = False
            if notnull_fields is not None and key in notnull_fields:
                field_notnull = True
            else:
                field_notnull = False
            fields.append(Field(key, type=field_types[key], unique=field_unique, notnull=field_notnull))

        # Skip for now - we won't have the original order at this point anyway
        #fields = [field for key in first_dict.keys() for field in fields if field.name == key]
        if len(fields) == 0:
            return None
        return fields

    @classmethod
    def _infer_field_type(cls, value) -> str:
        """
        Infer the field type for a given value.

        Parameters
        ----------
        value : any
            The value to infer the field type for

        Returns
        -------
        str
            The inferred field type for pydal
        """
<<<<<<< HEAD

=======
>>>>>>> 959cdc0 (- Add pydal table creation helpers.)
        # Quick checks for things we can't handle
        if value is None:
            return None
        elif isinstance(value, list):
            return False
        elif isinstance(value, dict):
            return False
        # isinstance() to check for object types is not reliable, so we check inclusively
        elif not isinstance(value, (int, float, bool, str, datetime)):
            return False

        # Quick checks for some intrinsic types
        if isinstance(value, datetime):
            return 'datetime'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, float):
            return 'double'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, str):
            if value == "":
                return 'string'

        # If we get here, it is a non-empty
        # Check for datetime type
        if is_date(value):
            return 'datetime'

        # Check for boolean type
        if value.lower() in ('true', 'false'):
            return 'boolean'
        elif value.lower() in ('y', 'n'):
            return 'boolean'
        elif value.lower() in ('yes', 'no'):
            return 'boolean'
        # Don't use 1/0 for now
        #elif value.lower() in ('1', '0'):
        #    return 'boolean'
        elif value.lower() in ('t', 'f'):
            return 'boolean'

        # Check for float type
        try:
            float(value)
            return 'double'
        except ValueError:
            pass

        # Check for integer type
        try:
            int(value)
            return 'integer'
        except ValueError:
            pass

        # Default to string for everything else
        return 'string'
