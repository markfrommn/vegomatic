#
# fileset - Operations on a collection of files.
#

#from . import ipmask
import glob
import os

class FileSet:
    def __init__(self):
        self.filepaths= []
        self.dirpath = ""
        self.globstr = "*.*"
        self.fullpath = ""
        self.iteridx = -1

    def glob(self, dirpath: str, globstr="*.*") -> int:
        self.dirpath = dirpath
        self.globstr = globstr
        if not os.path.isdir(self.dirpath):
            return None
        self.fullpath = "{}/{}".format(self.dirpath, self.globstr)
        fileiter = glob.iglob(self.fullpath)
        for path in fileiter:
            self.filepaths.append(path)
        return len(self.filepaths)

    def clear(self):
        self.filepaths = []

    def append(self, fp):
        self.filepaths.append(fp)

    def pop(self, idx: int) -> str:
        self.filepaths.pop(idx)

    def __iter__(self) -> object:
        self.iteridx = -1
        return self

    def __next__(self) -> str:
        if (self.iteridx + 1) >= len(self.filepaths):
            raise StopIteration
        self.iteridx += 1
        return self.filepaths[self.iteridx]




