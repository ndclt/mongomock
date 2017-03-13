
from uuid import UUID

from .py3compat import PY3

BINARY_SUBTYPE = 0
FUNCTION_SUBTYPE = 1
OLD_BINARY_SUBTYPE = 2
OLD_UUID_SUBTYPE = 3
UUID_SUBTYPE = 4
STANDARD = UUID_SUBTYPE
PYTHON_LEGACY = OLD_UUID_SUBTYPE
JAVA_LEGACY = 5
CSHARP_LEGACY = 6

ALL_UUID_SUBTYPES = (OLD_UUID_SUBTYPE, UUID_SUBTYPE)
ALL_UUID_REPRESENTATIONS = (STANDARD, PYTHON_LEGACY, JAVA_LEGACY, CSHARP_LEGACY)
UUID_REPRESENTATION_NAMES = {
    PYTHON_LEGACY: 'PYTHON_LEGACY',
    STANDARD: 'STANDARD',
    JAVA_LEGACY: 'JAVA_LEGACY',
    CSHARP_LEGACY: 'CSHARP_LEGACY'}

MD5_SUBTYPE = 5
USER_DEFINED_SUBTYPE = 128


class Binary(bytes):

    _type_marker = 5

    def __new__(cls, data, subtype=BINARY_SUBTYPE):
        if not isinstance(data, bytes):
            raise TypeError("data must be an instance of bytes")
        if not isinstance(subtype, int):
            raise TypeError("subtype must be an instance of int")
        if subtype >= 256 or subtype < 0:
            raise ValueError("subtype must be contained in [0, 256)")
        self = bytes.__new__(cls, data)
        self.__subtype = subtype
        return self

    @property
    def subtype(self):

        """Subtype of this binary data."""

        return self.__subtype

    def __getnewargs__(self):
        # Work around http://bugs.python.org/issue7382
        data = super(Binary, self).__getnewargs__()[0]
        if PY3 and not isinstance(data, bytes):
            data = data.encode('latin-1')
        return data, self.__subtype

    def __eq__(self, other):
        if isinstance(other, Binary):
            return ((self.__subtype, bytes(self)) ==
                    (other.subtype, bytes(other)))
        # We don't return NotImplemented here because if we did then
        # Binary("foo") == "foo" would return True, since Binary is a
        # subclass of str...
        return False

    def __hash__(self):
        return super(Binary, self).__hash__() ^ hash(self.__subtype)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Binary(%s, %s)" % (bytes.__repr__(self), self.__subtype)


class UUIDLegacy(Binary):

    def __new__(cls, obj):
        if not isinstance(obj, UUID):
            raise TypeError("obj must be an instance of uuid.UUID")
        self = Binary.__new__(cls, obj.bytes, OLD_UUID_SUBTYPE)
        self.__uuid = obj
        return self

    def __getnewargs__(self):
        # Support copy and deepcopy
        return (self.__uuid,)

    @property
    def uuid(self):

        """UUID instance wrapped by this UUIDLegacy instance."""

        return self.__uuid

    def __repr__(self):
        return "UUIDLegacy('%s')" % self.__uuid
