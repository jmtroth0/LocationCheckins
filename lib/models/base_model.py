from blist import sortedset
from datetime import datetime
from dateutil.tz import tzutc
from cassandra.util import OrderedMap
from cassandra.util import SortedSet
import json
from uuid import UUID


class JsonSerializableBase(object):
    """ Provides functions for serializing and deserializing objects to json.
    By default the initializer takes data that can be directly serialized.
    Inheriting classes can feel free override all methods if there are fields
    that are not json serializable.
    """

    def to_json(self):
        """ Convert object to json serialized string. """
        raise NotImplementedError("Subclass must implement")

    @classmethod
    def from_json(cls, value):
        """ Create object from json serialized string. """
        raise NotImplementedError("Subclass must implement")


class Field(object):
    BASE_TYPE = None

    def __init__(self, columnname=None, default=None):
        self.columnname = columnname

        # validate default type is appropriate for field
        if default is not None and not isinstance(default, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of %s" % (
                self.columnname, self.BASE_TYPE
            ))
        self.default = default

    def __get__(self, instance, objtype):
        value = instance._data.get(self.columnname)
        if value is not None:
            return value
        return self.default

    def __set__(self, instance, value):
        instance._data[self.columnname] = value

    def serialize(self, value):
        return value

class BooleanField(Field):
    BASE_TYPE = bool

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return
        if not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of bool" % (
                self.columnname
            ))

        instance._data[self.columnname] = value


class DateTimeField(Field):
    BASE_TYPE = datetime

    def __get__(self, instance, objtype):
        if not instance._data.get(self.columnname):
            return None
        return instance._data[self.columnname]

    def __set__(self, instance, value):
        if value and not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of datetime" % (
                self.columnname
            ))

        if value and value.tzinfo:
            # Since there is timezone info, we don't want to clobber
            instance._data[self.columnname] = value
        elif value:
            instance._data[self.columnname] = value.replace(tzinfo=tzutc())
        else:
            instance._data[self.columnname] = None


class IntegerField(Field):
    BASE_TYPE = int

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return
        if not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of int" % (
                self.columnname
            ))

        instance._data[self.columnname] = value


class FloatField(Field):
    BASE_TYPE = float

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return
        if not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of float" % (
                self.columnname
            ))

        instance._data[self.columnname] = value


class JsonWrappedClassField(Field):
    """ Wrapper class for data that is serialized to json and has an
    accompanying wrapper class which inherits `JsonSerializableBase`
    """

    def __init__(self, columnname=None, default_fn=None, wrapper_cls=None):
        """
        Arguments:
            columnname: Name of column in cassandra table definition.
            default_fn: Function for generating a default value.
            wrapper_cls: Class to wrap json data from column in.
        """
        self.BASE_TYPE = wrapper_cls
        super(JsonWrappedClassField, self).__init__(
            columnname=columnname,
            default=None
        )

        if wrapper_cls and not issubclass(wrapper_cls, JsonSerializableBase):
            raise Exception("Wrapper Class must inherit JsonSerializableBase")

        self._wrapper_cls = wrapper_cls
        self._default_fn = default_fn

    def __get__(self, instance, objtype):
        value = super(JsonWrappedClassField, self).__get__(instance, objtype)
        if value:
            return value

        if self._default_fn:
            return self._default_fn()

        return None

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return

        if isinstance(value, self._wrapper_cls):
            instance._data[self.columnname] = value
        elif isinstance(value, basestring):
            instance._data[self.columnname] = (
                self._wrapper_cls.from_json(value)
            )
        else:
            raise Exception("Unsupported type %s" % type(instance))

    def serialize(self, value):
        if not value:
            return None

        if not isinstance(value, self._wrapper_cls):
            raise Exception("Improper value set for class")

        return value.to_json()


class JsonDataField(Field):
    """ Wrapper class for data that is stored in a string field in cassandra
    but contains json serialized data.
    Allows modifying data in place without having to fumble with json
    deserialization for all data in that field.
    """
    BASE_TYPE = basestring

    def __init__(self, columnname=None, default=None):
        super(JsonDataField, self).__init__(
            columnname=columnname,
            default=default
        )

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return

        if not isinstance(value, (self.BASE_TYPE, dict, list)):
            raise Exception(
                "Property %s must be an instance of str, dict, or list" % (
                    self.columnname
                )
            )

        if isinstance(value, self.BASE_TYPE):
            decoded = json.loads(value)

            if not isinstance(decoded, (dict, list)):
                raise Exception(
                    "Decoded Json in Property %s must be an instance of "
                    "dict or list" % (
                        self.columnname
                    )
                )

            instance._data[self.columnname] = decoded
            return

        instance._data[self.columnname] = value

    def serialize(self, value):
        if not value:
            return None

        return json.dumps(value)


class StringField(Field):
    BASE_TYPE = basestring

    def __set__(self, instance, value):
        if value and not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of basestring" % (
                self.columnname
            ))
        instance._data[self.columnname] = value


class MapField(Field):
    BASE_TYPE = dict

    def __set__(self, instance, value):
        if isinstance(value, OrderedMap):
            value = dict(value.iteritems())
        if value and not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of dict" % (
                self.columnname
            ))

        if value is None:  # explicit check for None
            instance._data[self.columnname] = dict()
            return

        instance._data[self.columnname] = value

    def __get__(self, instance, objtype):
        if instance._data.get(self.columnname) is None:  # explicit None check
            if self.default:
                instance._data[self.columnname] = self.default
            else:
                instance._data[self.columnname] = dict()
        return instance._data[self.columnname]


class SetField(Field):
    BASE_TYPE = set

    def __set__(self, instance, value):
        if value and not (isinstance(value, set) or
                          isinstance(value, sortedset) or
                          isinstance(value, SortedSet)):
            raise Exception("Property %s must be an instance of set, "
                            "sortedset, or SortedSet" % (self.columnname))

        if value is None:  # explicit check for None
            instance._data[self.columnname] = set()
            return

        instance._data[self.columnname] = value

    def __get__(self, instance, objtype):
        if instance._data.get(self.columnname) is None:  # explicit None check
            if self.default:
                instance._data[self.columnname] = self.default
            else:
                instance._data[self.columnname] = set()
        return instance._data[self.columnname]


class UuidField(Field):
    BASE_TYPE = UUID

    def __set__(self, instance, value):
        if value is None:
            instance._data[self.columnname] = None
            return
        if not isinstance(value, self.BASE_TYPE):
            raise Exception("Property %s must be an instance of int" % (
                self.columnname
            ))

        instance._data[self.columnname] = value

    def __get__(self, instance, objtype):
        if instance._data.get(self.columnname) is None:  # explicit None check
            if self.default:
                instance._data[self.columnname] = self.default
            else:
                instance._data[self.columnname] = None
        return instance._data[self.columnname]


class ModelMeta(type):

    _registry = {}

    def __new__(cls, name, bases, attrs):
        if name not in cls._registry:
            cls._registry[name] = {}
        for attrname, field_obj in attrs.items():
            if isinstance(field_obj, Field):
                # use name of attribute if there is no columnname
                if not field_obj.columnname:
                    field_obj.columnname = attrname
                cls._registry[name][attrname] = field_obj
        return super(ModelMeta, cls).__new__(cls, name, bases, attrs)


class Model(object):
    __metaclass__ = ModelMeta

    def __init__(self, data=None):
        self._data = {}

        if not data:
            return

        attributes = self.attributes

        for key, value in data.iteritems():
            if key not in attributes:
                raise Exception("Invalid attribute %s" % key)
            setattr(self, key, value)

    def __repr__(self):
        str = "<%s " % self.__class__.__name__
        pairs = []
        for attr in self.attributes:
            if not attr.startswith("_"):
                pairs.append("%s=%s" % (attr, repr(getattr(self, attr))))
        str += ' '.join(pairs)
        str += ">"
        return str

    @property
    def attributes(self):
        """ returns list of attributes registered on the model """
        return sorted(ModelMeta._registry[self.__class__.__name__])

    def attrnames_and_columnnames(self):
        """ returns mapping dict where the key is the attribute name on
        the object, and the value is the database columnname """
        r = ModelMeta._registry[self.__class__.__name__]
        mapping = {}
        for attrname, fieldobj in r.iteritems():
            mapping[attrname] = fieldobj.columnname
        return mapping

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        for attr in self.attributes:
            if getattr(self, attr) != getattr(other, attr):
                return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_dict(self):
        return {
            attr: getattr(self, attr) for attr in self.attributes
        }

    def serialize(self):
        """ Serialize all fields for writing to cassandra.
        Similar to the `to_dict` method, but some fields require certain
        serialization to properly write to cassandra.
        """
        r = ModelMeta._registry[self.__class__.__name__]
        serialized = {}
        for attrname, fieldobj in r.iteritems():
            serialized_value = fieldobj.serialize(getattr(self, attrname))
            serialized[fieldobj.columnname] = serialized_value

        return serialized