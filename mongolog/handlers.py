# -*- coding: utf-8 *-*
import sys
import getpass
import logging
import re

from bson import InvalidDocument
from datetime import datetime
from pymongo.collection import Collection
from socket import gethostname

try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection

if sys.version_info[0] >= 3:
    unicode = str

class MongoFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        super(MongoFormatter, self).__init__(fmt, datefmt)
        if fmt:
            pattern = re.compile(r'(?<=\()(.+?)(?=\))')
            result = re.findall(pattern, fmt)
            result.remove('message')
            result.append('msg')
            self._fmt = result

    def format(self, record):
        """Format exception object as a string"""
        data = record.__dict__.copy()

        if record.args:
            msg = record.msg % record.args
        else:
            msg = record.msg

        data = { k:data[k] for k in set(self._fmt) & set(data.keys()) }
        if 'username' in data:
            data['username'] = getpass.getuser()
        if 'time' in data:
            data['time'] = datetime.now()
        if 'host' in data:
            data['host'] = gethostname()
        if 'args' in data:
            data['args'] = tuple(unicode(arg) for arg in record.args)
        return data

class MongoHandler(logging.Handler):
    """ Custom log handler

    Logs all messages to a mongo collection. This  handler is
    designed to be used with the standard python logging mechanism.
    """

    @classmethod
    def to(cls, collection, db='mongolog', host='localhost', port=None,
        username=None, password=None, level=logging.NOTSET):
        """ Create a handler for a given  """
        return cls(collection, db, host, port, username, password, level)

    def __init__(self, collection, db='mongolog', host='localhost', port=None,
        username=None, password=None, level=logging.NOTSET):
        """ Init log handler and store the collection handle """
        logging.Handler.__init__(self, level)
        if isinstance(collection, str):
            connection = Connection(host, port)
            if username and password:
                connection[db].authenticate(username, password)
            self.collection = connection[db][collection]
        elif isinstance(collection, Collection):
            self.collection = collection
        else:
            raise TypeError('collection must be an instance of basestring or '
                             'Collection')
        self.formatter = MongoFormatter()

    def emit(self, record):
        """ Store the record to the collection. Async insert """
        try:
            self.collection.insert(self.format(record))
        except InvalidDocument as e:
            logging.error("Unable to save log record: %s", e.message,
                exc_info=True)
