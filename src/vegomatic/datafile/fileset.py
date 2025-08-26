#
# fileset - Operations on a collection of files.
#

#from . import ipmask
import glob
import os

class FileSet:
    """
    A collection of file paths with operations for managing and iterating over them.
    """

    def __init__(self):
        """
        Initialize an empty FileSet.
        """
        self.filepaths = []
        self.dirpath = ""
        self.globstr = "*.*"
        self.fullpath = ""
        self.iteridx = -1

    def glob(self, dirpath: str, globstr="*.*") -> int:
        """
        Populate the FileSet with files matching a glob pattern.

        Args:
            dirpath: Directory path to search in
            globstr: Glob pattern to match files (default: "*.*")

        Returns:
            int: Number of files found and added to the set

        Note:
            If the directory doesn't exist, returns None.
        """
        self.dirpath = dirpath
        self.globstr = globstr
        if not os.path.isdir(self.dirpath):
            return None
        self.fullpath = "{}/{}".format(self.dirpath, self.globstr)
        fileiter = glob.iglob(self.fullpath)
        for path in fileiter:
            self.filepaths.append(path)
        return len(self.filepaths)

    def clear(self) -> None:
        """
        Clear all file paths from the FileSet.
        """
        self.filepaths = []

    def append(self, fp: str) -> None:
        """
        Add a file path to the FileSet.

        Args:
            fp: File path to add
        """
        self.filepaths.append(fp)

    def pop(self, idx: int) -> str:
        """
        Remove and return a file path at the specified index.

        Args:
            idx: Index of the file path to remove

        Returns:
            str: The removed file path
        """
        return self.filepaths.pop(idx)

    def __iter__(self) -> object:
        """
        Initialize iteration over the FileSet.

        Returns:
            self: The FileSet instance for iteration
        """
        self.iteridx = -1
        return self

    def __next__(self) -> str:
        """
        Get the next file path in the iteration.

        Returns:
            str: The next file path

        Raises:
            StopIteration: When there are no more file paths to iterate over
        """
        if (self.iteridx + 1) >= len(self.filepaths):
            raise StopIteration
        self.iteridx += 1
        return self.filepaths[self.iteridx]




