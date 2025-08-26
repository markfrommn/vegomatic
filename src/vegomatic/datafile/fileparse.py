#
# fileparse - A set of utilities for parsing files.
#

import csv
import io
import json
import os
import sys
from typing import Dict, List, Mapping, Tuple, Union
from urllib import parse

from . import FileSet

def dict_flatten_values(adict: Mapping) -> dict:
    """
    Flatten a dictionary by taking the first value from each list.

    Args:
        adict: Dictionary where values are lists

    Returns:
        dict: Flattened dictionary with single values instead of lists
    """
    newdict = {}
    for key, vals in adict.items():
        # Force key to string
        ikey = str(key)
        newdict[ikey] = vals[0]
    return newdict


def dict_from_kvpfile(filepath: str) -> dict:
    """
    Parse a key-value pair file into a dictionary.

    Args:
        filepath: Path to the key-value pair file

    Returns:
        dict: Dictionary containing the key-value pairs

    Note:
        The file should contain lines in the format "key=value".
        Empty lines are ignored.
    """
    kvps = {}
    kvpfile = open(filepath, "r")
    for aline in kvpfile:
        bline = str.strip(aline)
        if bline != "":
            if "=" in bline:
                kvpair = bline.split("=",2)
                if len(kvpair) == 2:
                    # Force key to string
                    key = str(kvpair[0])
                    kvps[key] = kvpair[1]
    if len(kvps) == 0:
        print("Line splitting failed on {}\n".format(filepath))
        sys.exit(40)
    kvpfile.close()
    return kvps


def dict_from_urlfile(filepath: str) -> dict:
    """
    Parse a URL-encoded file into a dictionary.

    Args:
        filepath: Path to the URL-encoded file

    Returns:
        dict: Dictionary containing the parsed URL parameters

    Note:
        The file should contain URL-encoded strings, one per line.
        Each line is parsed as URL query parameters.
    """
    kvps = {}
    urlfile = open(filepath, "r")
    for aline in urlfile:
        linepairs = parse.parse_qs(aline, True)
        kvps.update(linepairs)
    urlfile.close()
    return dict_flatten_values(kvps)


def dicts_from_files(afileset: FileSet, keyprop: str, filetype="kvp") -> Tuple[dict, list]:
    """
    Parse multiple files into dictionaries using a specified key property.

    Args:
        afileset: FileSet containing the files to parse
        keyprop: Property name to use as the dictionary key
        filetype: Type of file to parse ("kvp" or "url")

    Returns:
        Tuple[dict, list]: Tuple containing (dictionary of parsed data, list of items without keys)

    Raises:
        NotImplementedError: If filetype is not supported
    """
    if "kvp" == filetype:
        ffunc = dict_from_kvpfile
    elif "url" == filetype:
        ffunc = dict_from_urlfile
    else:
        print("Unknown file type {}\n".format(filetype))
        raise NotImplementedError
    dicts = {}
    nokeys = []
    for path in afileset:
        kvpset = ffunc(path)
        if keyprop in kvpset:
            # Force keys to string so the dict is sortable
            # Some PP txn IDs will be all digits so will be int/floats by default
            dkey = str(kvpset[keyprop])
            dicts[dkey] = kvpset
        else:
            nokeys.append(kvpset)
    return  (dicts, nokeys)


def data_from_json_file(filepath: str) -> Union[dict, list, object]:
    """
    Load data from a JSON file.

    Args:
        filepath: Path to the JSON file

    Returns:
        Union[dict, list, object]: The parsed JSON data

    Raises:
        SystemExit: If the JSON file is empty or invalid
    """
    kvpfile = open(filepath, "r")
    anobj = json.load(kvpfile)
    kvpfile.close()
    if len(anobj) == 0:
        print("Load JSON failed on {}\n".format(filepath))
        sys.exit(40)
    return anobj


def dictlist_from_csv_stream(csvio) -> list:
    """
    Parse CSV data from a stream into a list of dictionaries.

    Args:
        csvio: File-like object containing CSV data

    Returns:
        list: List of dictionaries, one per row
    """
    retrows = []
    # csvdialect = csv.Sniffer().sniff(csvio.read(1024))
    # csvio.seek(0)
    reader = csv.DictReader(csvio, fieldnames=None, dialect='excel')
    for row in reader:
        retrows.append(row)
    return retrows


def dictlist_from_csv_str(csvbuf: str) -> list:
    """
    Parse CSV data from a string into a list of dictionaries.

    Args:
        csvbuf: String containing CSV data

    Returns:
        list: List of dictionaries, one per row
    """
    retrows = []
    with io.StringIO(csvbuf) as csvfile:
        retrows = dictlist_from_csv_stream(csvfile)
    return retrows


def dictlist_from_csv_file(path: str) -> list:
    """
    Parse CSV data from a file into a list of dictionaries.

    Args:
        path: Path to the CSV file

    Returns:
        list: List of dictionaries, one per row
    """
    retrows = []
    with open(path, newline='') as csvfile:
        retrows = dictlist_from_csv_stream(csvfile)
    return retrows


def column_from_csv_str(csvbuf: str, colnum: int) -> list:
    """
    Extract a specific column from CSV data in a string.

    Args:
        csvbuf: String containing CSV data
        colnum: Column number to extract (0-based)

    Returns:
        list: List of values from the specified column
    """
    retvals = []
    with io.StringIO(csvbuf) as csvfile:
        fieldreader = csv.reader(csvfile)
        for row in fieldreader:
            if colnum < len(row):
                retvals.append(row[colnum])
    return retvals


def column_from_csv_file(path: str, colnum: int) -> list:
    """
    Extract a specific column from a CSV file.

    Args:
        path: Path to the CSV file
        colnum: Column number to extract (0-based)

    Returns:
        list: List of values from the specified column
    """
    retrows = []
    with open(path, newline='') as csvfile:
        retrows = column_from_csv_str(csvfile, colnum)
    return retrows


def data_to_json_file(filepath: str, odata: Union[dict, list[dict]]) -> None:
    """
    Write data to a JSON file.

    Args:
        filepath: Path to the output JSON file
        odata: Data to write (dictionary or list of dictionaries)
    """
    jsonfile = open(filepath, "w")
    json.dump(odata, jsonfile, sort_keys=True, indent=4)
    jsonfile.close()

def dictionary_to_json_files(dirpath: str, adict: Mapping[str, dict]) -> None:
    """
    Write a dictionary to a directory of JSON files.

    Args:
        dirpath: Directory path where JSON files will be created
        adict: Dictionary mapping keys to data dictionaries

    Note:
        Each key-value pair in the dictionary will be written to a separate
        JSON file named "{key}.json" in the specified directory.
    """
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    for key, val in adict.items():
        filepath = f"{dirpath}/{key}.json"
        data_to_json_file(filepath, val)
