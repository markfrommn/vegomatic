#
# fileparse - A set of utilities for parsing files.
#

import sys
import io
from typing import Callable, Union, Tuple, List
from urllib import parse
import json
import csv
from . import FileSet

def dict_flatten_values(adict: dict) -> dict:
    """

    :param adict:
    :return:
    """
    newdict = {}
    for key, vals in adict.items():
        # Force key to string
        ikey = str(key)
        newdict[ikey] = vals[0]
    return newdict


def dict_from_kvpfile(filepath: str) -> dict:
    """

    :type filepath:
    :param filepath:
    :return: 
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

    :param filepath: 
    :return: 
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

    :param afileset:
    :param keyprop:
    :param filetype:
    :return:
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

    :type filepath:
    :param filepath:
    :return:
    """
    kvpfile = open(filepath, "r")
    anobj = json.load(kvpfile)
    kvpfile.close()
    if len(anobj) == 0:
        print("Load JSON failed on {}\n".format(filepath))
        sys.exit(40)
    return anobj


def dictlist_from_csv_stream(csvio) -> list:
    retrows = []
    #csvdialect = csv.Sniffer().sniff(csvio.read(1024))
    #csvio.seek(0)
    reader = csv.DictReader(csvio, fieldnames=None, dialect='excel')
    for row in reader:
        retrows.append(row)
    return retrows


def dictlist_from_csv_str(csvbuf: str) -> list:
    retrows = []
    with io.StringIO(csvbuf) as csvfile:
        retrows = dictlist_from_csv_stream(csvfile)
    return retrows


def dictlist_from_csv_file(path: str) -> list:
    retrows = []
    with open(path, newline='') as csvfile:
        retrows = dictlist_from_csv_stream(csvfile)
    return retrows


def column_from_csv_str(csvbuf: str, colnum: int) -> list:
    retvals = []
    with io.StringIO(csvbuf) as csvfile:
        fieldreader = csv.reader(csvfile)
        for row in fieldreader:
            if colnum < len(row):
                retvals.append(row[colnum])
    return retvals


def column_from_csv_file(path: str, colnum: int) -> list:
    retrows = []
    with open(path, newline='') as csvfile:
        retrows = column_from_csv_str(csvfile, colnum)
    return retrows


def data_to_json_file(filepath: str, odata: Union[dict, List[dict]]):
    """
    Write data to a JSON file
    :type filepath:
    :param filepath:
    :return:
    """
    jsonfile = open(filepath, "w")
    json.dump(odata, jsonfile, sort_keys=True, indent=4)
    jsonfile.close()
