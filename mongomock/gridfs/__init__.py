"""MockGridFS A GridFS implementation on top of mongomock
"""

from collections import Mapping

from .errors import NoFile
from .grid_file import (GridIn,
                        GridOut,
                        GridOutCursor,
                        DEFAULT_CHUNK_SIZE)

from mongomock.helpers import (ASCENDING,
                               DESCENDING)

from mongomock import ConfigurationError, Database
from mongomock.bson.py3compat import string_type


def validate_string(option, value):
    if isinstance(value, string_type):
        return value
    raise TypeError("Wrong type for %s, value must be "
                    "an instance of %s" % (option, string_type.__name__))


class MockGridFS(object):

    """An instance of GridFS on top of a single Database."""

    def __init__(self, database, collection="fs"):
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        if not database.write_concern.acknowledged:
            raise ConfigurationError('database must use '
                                     'acknowledged write_concern')

        self.__database = database
        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks

    def new_file(self, **kwargs):
        return GridIn(self.__collection, **kwargs)

    def put(self, data, **kwargs):
        grid_file = GridIn(self.__collection, **kwargs)
        try:
            grid_file.write(data)
        finally:
            grid_file.close()

        return grid_file._id

    def get(self, file_id):
        gout = GridOut(self.__collection, file_id)

        gout._ensure_file()
        return gout

    def get_version(self, filename=None, version=-1, **kwargs):
        query = kwargs
        if filename is not None:
            query["filename"] = filename

        cursor = self.__files.find(query)
        if version < 0:
            skip = abs(version) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(version).sort("uploadDate", ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self.__collection, file_document=grid_file)
        except StopIteration:
            raise NoFile("no version %d for filename %r" % (version, filename))

    def get_last_version(self, filename=None, **kwargs):
        return self.get_version(filename=filename, **kwargs)

    def delete(self, file_id):
        self.__files.delete_one({"_id": file_id})
        self.__chunks.delete_many({"files_id": file_id})

    def list(self):
        # With an index, distinct includes documents with no filename
        # as None.
        return [
            name for name in self.__files.distinct("filename")
            if name is not None]

    def find_one(self, filter=None, *args, **kwargs):
        if filter is not None and not isinstance(filter, Mapping):
            filter = {"_id": filter}

        for f in self.find(filter, *args, **kwargs):
            return f

        return None

    def find(self, *args, **kwargs):
        return GridOutCursor(self.__collection, *args, **kwargs)

    def exists(self, document_or_id=None, **kwargs):
        if kwargs:
            return self.__files.find_one(kwargs, ["_id"]) is not None
        return self.__files.find_one(document_or_id, ["_id"]) is not None


class GridFSBucket(object):

    """An instance of MockGridFS on top of a single mock Database."""

    def __init__(self, db, bucket_name="fs",
                 chunk_size_bytes=DEFAULT_CHUNK_SIZE, write_concern=None,
                 read_preference=None):
        if not isinstance(db, Database):
            raise TypeError("database must be an instance of mongomock Database")

        wtc = write_concern if write_concern is not None else db.write_concern
        if not wtc.acknowledged:
            raise ConfigurationError('write concern must be acknowledged')

        self._db = db
        self._bucket_name = bucket_name
        self._collection = db[bucket_name]

        self._chunks = self._collection.chunks.with_options(
            write_concern=write_concern,
            read_preference=read_preference)

        self._files = self._collection.files.with_options(
            write_concern=write_concern,
            read_preference=read_preference)

        self._chunk_size_bytes = chunk_size_bytes

    def open_upload_stream(self, filename, chunk_size_bytes=None,
                           metadata=None):
        validate_string("filename", filename)

        opts = {"filename": filename,
                "chunk_size": (chunk_size_bytes if chunk_size_bytes
                               is not None else self._chunk_size_bytes)}
        if metadata is not None:
            opts["metadata"] = metadata

        return GridIn(self._collection, **opts)

    def open_upload_stream_with_id(
            self, file_id, filename, chunk_size_bytes=None, metadata=None):
        validate_string("filename", filename)

        opts = {"_id": file_id,
                "filename": filename,
                "chunk_size": (chunk_size_bytes if chunk_size_bytes
                               is not None else self._chunk_size_bytes)}
        if metadata is not None:
            opts["metadata"] = metadata

        return GridIn(self._collection, **opts)

    def upload_from_stream(self, filename, source, chunk_size_bytes=None,
                           metadata=None):
        with self.open_upload_stream(
                filename, chunk_size_bytes, metadata) as gin:
            gin.write(source)

        return gin._id

    def upload_from_stream_with_id(self, file_id, filename, source,
                                   chunk_size_bytes=None, metadata=None):
        with self.open_upload_stream_with_id(
                file_id, filename, chunk_size_bytes, metadata) as gin:
            gin.write(source)

    def open_download_stream(self, file_id):
        gout = GridOut(self._collection, file_id)

        # Raise NoFile now, instead of on first attribute access.
        gout._ensure_file()
        return gout

    def download_to_stream(self, file_id, destination):
        gout = self.open_download_stream(file_id)
        for chunk in gout:
            destination.write(chunk)

    def delete(self, file_id):
        res = self._files.delete_one({"_id": file_id})
        self._chunks.delete_many({"files_id": file_id})
        if not res.deleted_count:
            raise NoFile(
                "no file could be deleted because none matched %s" % file_id)

    def find(self, *args, **kwargs):
        return GridOutCursor(self._collection, *args, **kwargs)

    def open_download_stream_by_name(self, filename, revision=-1):
        validate_string("filename", filename)

        query = {"filename": filename}

        cursor = self._files.find(query)
        if revision < 0:
            skip = abs(revision) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(revision).sort("uploadDate", ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self._collection, file_document=grid_file)
        except StopIteration:
            raise NoFile(
                "no version %d for filename %r" % (revision, filename))

    def download_to_stream_by_name(self, filename, destination, revision=-1):
        gout = self.open_download_stream_by_name(filename, revision)
        for chunk in gout:
            destination.write(chunk)

    def rename(self, file_id, new_filename):
        result = self._files.update_one({"_id": file_id},
                                        {"$set": {"filename": new_filename}})
        if not result.matched_count:
            raise NoFile("no files could be renamed %r because none "
                         "matched file_id %i" % (new_filename, file_id))
