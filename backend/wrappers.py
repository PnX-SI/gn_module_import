import datetime
from .logs import logger


def checker(message):
    def timer(func):
        def wrapper(*args, **kw):
            start = datetime.datetime.now()
            res = func(*args, **kw)
            end = datetime.datetime.now()
            logger.info('{} - time : {}'.format(message, end-start))
            return res
        return wrapper
    return timer