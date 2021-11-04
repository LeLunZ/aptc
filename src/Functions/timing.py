import logging
from functools import wraps
from time import time

logger = logging.getLogger(__name__)


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logger.debug(f'Timing - func:{f.__name__} args:[{args}, {kw}] took: i%2.4f sec' % (te - ts))
        return result

    return wrap
