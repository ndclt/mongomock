
from mongomock import PyMongoError


class GridFSError(PyMongoError):
    """Base class for all GridFS exceptions."""


class CorruptGridFile(GridFSError):
    """Raised when a file in :class:`~gridfs.GridFS` is malformed."""


class NoFile(GridFSError):
    """Raised when trying to read from a non-existent file."""


class FileExists(GridFSError):
    """Raised when trying to create a file that already exists."""
