

# File: logger.py

"""
For example:
    import logger
    logger.session.trace_id = 'test123'
    logger.info("Test info.")
    logger.error("Error happened!")
"""


import logging
import os
import sys
from logging import LoggerAdapter


__all__ = []

levelname = os.environ.get('LOG_LEVEL', "INFO")

_LEVEL_SET = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}
_LOGGING_METHOD = ["info", "warning", "error", "debug"]


class _Formatter(logging.Formatter):
    def format(self, record):
        msg = "%(message)s"
        pattern = "%(asctime)s.%(msecs)03d %(levelname)s [pid-%(process)d] @%(filename)s:%(lineno)d"
        fmt = pattern + " " + msg
        if hasattr(self, "_style"):
            self._style._fmt = fmt
        self._fmt = fmt
        return super(_Formatter, self).format(record)


class _SesssionLoggerAdapter(LoggerAdapter):

    def process(self, msg, kwargs):
        if 'session' not in self.extra or self.extra['session'] is None:
            return msg, kwargs
        session = self.extra['session']
        if hasattr(session, 'trace_id'):
            msg = '{} {}'.format(session.trace_id, msg)
        if 'extra' not in kwargs:
            kwargs["extra"] = self.extra
        else:
            kwargs['extra'].update(self.extra)
        return super().process(msg, kwargs)


def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class Session():
    def __init__(self):
        super().__init__()

    @property
    def trace_id(self):
        return self._trace_id

    @trace_id.setter
    def trace_id(self, trace_id):
        self._trace_id = trace_id


def _getlogger():
    package_name = "http_serving"
    logger = logging.getLogger(package_name)
    logger.propagate = False
    logger.setLevel(_LEVEL_SET.get(levelname))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_Formatter(datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    return logger


# Logger hander callback
session = Session()


# Logger instance
_logger = _getlogger()
_logger = _SesssionLoggerAdapter(_logger, {'session': session})


# Export logger functions
for func in _LOGGING_METHOD:
    locals()[func] = getattr(_logger, func)
    __all__.append(func)

