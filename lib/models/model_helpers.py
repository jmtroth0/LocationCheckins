from datetime import datetime
from cassandra.util import OrderedMap
from dateutil.tz import tzutc


def row_to_dict(row):
    """ Takes a namedtuple result from a cassandra
    query and converts it into a form that is ammenable
    to insert into a model.
    """
    d = {}
    for name, attr in row._asdict().iteritems():
        if isinstance(attr, OrderedMap):
            attr = dict(attr.iteritems())
            # adds timezone info for dicts with datetimes
            # fields that are just datetimes have this info added in
            # CassandraModel DateTimeField
            for k in attr.keys():
                v = attr[k]
                if isinstance(v, datetime) and not v.tzinfo:
                    v = v.replace(tzinfo=tzutc())
                    attr[k] = v
        d[name] = attr

    return d
