# -*- coding: utf-8 *-*
'''
define the formatter and handler of MongoDB.
'''
import sys
import getpass
import logging
import re

from bson import InvalidDocument
from datetime import datetime
from socket import gethostname
from mongoengine.connection import get_db

try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection

if sys.version_info[0] >= 3:
    unicode = str

class Formatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, **kwargs):
        super(Formatter, self).__init__(fmt, datefmt)
        if fmt:
            pattern = re.compile(r'(?<=\()(.+?)(?=\))')
            allKeys = r'''%(username)s %(name)s %(host)s
                %(time)s %(asctime)s %(msecs)d %(relativeCreated)d %(created)f
                %(processName)s %(process)d %(threadName)s %(thread)d
                %(module)s %(pathname)s %(filename)s %(lineno)d %(funcName)s
                %(levelname)s %(levelno)d
                %(msg)s %(message)s %(args)s
                %(exc_info)s %(exc_text)s'''
            allKeys = set(re.findall(pattern, allKeys))
            fmtKeys = set(re.findall(pattern, fmt))
            self._keySet = fmtKeys & allKeys

    def format(self, record):
        """Format exception object as a string"""
        if hasattr(self, '_keySet'):
            data = {}
            for k in self._keySet:
                try:
                    data[k] = record.__dict__[k]
                except KeyError:
                    pass

            if 'username' in self._keySet:
                data['username'] = getpass.getuser()
            if 'time' in self._keySet:
                data['time'] = datetime.now()
            if 'host' in self._keySet:
                data['host'] = gethostname()
            if 'message' in self._keySet:
                data['message'] = record.msg
            if 'args' in self._keySet:
                data['args'] = tuple(unicode(arg) for arg in record.args)

            return data

        else:
            data = record.__dict__.copy()

            if record.args:
                msg = record.msg % record.args
            else:
                msg = record.msg

            data.update(
                username=getpass.getuser(),
                time=datetime.now(),
                host=gethostname(),
                message=msg,
                args=tuple(unicode(arg) for arg in record.args)
            )
            return data


class Handler(logging.Handler):
    """ Custom log handler
    Logs all messages to a mongo collection. This  handler is
    designed to be used with the standard python logging mechanism.
    """
    def __init__(self, collection='log', db='mongolog', host='localhost', port=None,
        username=None, password=None, level=logging.NOTSET):
        """ Init log handler and store the collection handle """
        logging.Handler.__init__(self, level)
        connection = get_db(db)
        self.collection = connection[collection]

    def emit(self, record):
        """ Store the record to the collection. Async insert """
        try:
            self.collection.insert(self.format(record))
        except InvalidDocument as e:
            logging.error("Unable to save log record: %s", e.message,
                exc_info=True)
