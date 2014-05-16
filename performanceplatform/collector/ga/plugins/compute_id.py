import base64
import datetime

import pytz


def to_utc(a_datetime):
    return a_datetime.astimezone(pytz.UTC)


def value_id(value):
    value_bytes = value.encode('utf-8')
    return base64.urlsafe_b64encode(value_bytes), value_bytes


class ComputeIdFrom(object):

    def __init__(self, *fields):
        self.fields = fields

    def __call__(self, documents):
        for document in documents:

            id_parts = (stringify(document[field]) for field in self.fields)
            id_string = "_".join(id_parts)

            _id, humanId = value_id(id_string)

            document['_id'] = _id
            document['humanId'] = humanId

        return documents


def stringify(item):
    if isinstance(item, datetime.datetime):
        return _format(item)
    return u"{0!s}".format(item)


def _format(timestamp):
    return to_utc(timestamp).strftime("%Y%m%d%H%M%S")
