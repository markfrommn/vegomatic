#
# objutils - Various object utilities.
#

from collections import namedtuple
from typing import Union
from types import SimpleNamespace
from copy import copy
import inspect

def ikeyval(inkey):
    keyval = inkey
    if is_number(inkey) and inkey > 1000000000:
        keyval = str(inkey)
    return keyval

def fixedobj_from_dict(aname: str, indict: dict, normalize=True) -> object:
    adict = copy(indict)
    if normalize:
        for key, val in adict.items():
            adict[ikeyval(key)] = normalize_value(val)
    retobj  = namedtuple(aname, adict.keys())(*adict.values())
    return retobj


def dict_from_object(anobj: object, fieldlist=None, missing=None) -> dict:
    retdict = {}
    if fieldlist is None:
        fieldlist = anobj.__dict__.keys()
    for fname in fieldlist:
        fval = getattr(anobj, fname, missing)
        retdict[fname] = fval
    return retdict


def dynobj_from_dict(indict: dict, normalize=True) -> object:
    adict = copy(indict)
    if normalize:
        for key, val in adict.items():
            adict[ikeyval(key)] = normalize_value(val)
    retobj  = SimpleNamespace()
    for key, val in adict.items():
        setattr(retobj, ikeyval(key), val)
    return retobj


def objlist_from_dictlist(aname: str, alist: list) -> list:
    retlist = []
    for ditem in alist:
        oitem = dynobj_from_dict(ditem)
        retlist.append(oitem)
    return retlist


def getvalue(anitem: Union[dict,object], keyprop:str):
    if inspect.isclass(type(anitem)):
        keyval = getattr(anitem, keyprop, None)
    elif type(anitem) == dict:
        if keyprop == dict:
            keyval = anitem[keyprop]
        else:
            keyval = None
    else:
        raise TypeError
    return keyval


def dict_from_list(alist: list, keyprop: str, ignore_missing_key=True) -> dict:
    retdict = {}
    for anitem in alist:
        keyval = getvalue(anitem, keyprop)
        if keyval is None:
            if not ignore_missing_key:
                raise KeyError
            else:
                continue
        # Force key to string if it looks like a really big int - fudge for UUID/TXND type keys
        keyval = ikeyval(keyval)
        retdict[keyval] = anitem
    return retdict


def list_find_by_prop(alist: list, propname: str, propval: Union[str,int,float]):
    for item in alist:
        if propname not in item:
            continue
        if item[propname] == propval:
            return item
    return None


def is_number(s:Union(None,str)) -> bool:
    if s is None:
        return False
    nval = num_value(s)
    if isinstance(nval, (float,int)):
        return True
    return False


def num_value(s:str) -> Union[float,int]:
    nval = normalize_value(s)
    if isinstance(nval, (float,int)):
        return nval
    return None

# Normalize simple values to integral int or float type if possible
def normalize_value(s:Union(None,str)) -> Union[float,int,str]:
    if isinstance(s, (None,dict,list,object)):
        return None

    try:
        nval = int(s)
        if nval.is_integer():
            return nval
    except ValueError:
        pass

    try:
        nval = float(s)
        return nval
    except ValueError:
        pass

    return s

# Return a new list sorted by the keylist from a dict
def sort_list(adict: dict, keyseq: List(str)) -> list:
    return sorted(adict, key=lambda x: [x[key] for key in keyseq])
