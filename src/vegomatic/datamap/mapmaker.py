
#
# mapmaker - various tools to transform data
#

from datafile import dictlist_from_csv_str
from datamap import dynobj_from_dict
import ciso8601
import time


def object_convert_with_map(map: str, inobj: object) -> object:
    retdict = {}
    mapping = dictlist_from_csv_str(map)
    for field in mapping:
        toname = field.get("tofield", None)
        action = field.get("convert", None)
        if toname is None or action is None:
            continue
        if action == "" or action == "ignore":
            retdict[toname] = ""
        elif action == "copy" or action == "copy-unixtime":
            fromname = field.get("fromfield", None)
            if fromname is None:
                raise KeyError
            newval = getattr(inobj, fromname, None)
            if newval is None:
                newval = ""
            else:
                if action == "copy-unixtime":
                    ts = ciso8601.parse_datetime(newval)
                    # to get time in seconds:
                    newval = time.mktime(ts.timetuple())
            retdict[toname] = newval
        elif action == "copyif":
            fromnames1 = field.get("fromfield", None)
            if fromnames1 is None:
                raise KeyError
            retdict[toname] = ""
            fromnames = fromnames1.split("-")
            for fromname in fromnames:
                newval = getattr(inobj, fromname, None)
                if newval is not None:
                    retdict[toname] = newval
                    break
        elif action == "copyif-unixtime":
            fromnames1 = field.get("fromfield", None)
            if fromnames1 is None:
                raise KeyError
            retdict[toname] = ""
            fromnames = fromnames1.split("-")
            for fromname in fromnames:
                newval = getattr(inobj, fromname, None)
                if newval is not None:
                    ts = ciso8601.parse_datetime(newval)
                    # to get time in seconds:
                    newval = time.mktime(ts.timetuple())
                    retdict[toname] = newval
                    break
        elif action == "set":
            newval = field.get("value", "")
            retdict[toname] = newval
        else:
            raise KeyError("Invalid action {}".format(action))
    retobj = dynobj_from_dict(retdict, True)
    return retobj


def object_empty_with_map(map: str) -> object:
    retdict = {}
    mapping = dictlist_from_csv_str(map)
    for field in mapping:
        toname = field.get("tofield", None)
        retdict[toname] = None
    retobj = dynobj_from_dict(retdict, True)
    return retobj