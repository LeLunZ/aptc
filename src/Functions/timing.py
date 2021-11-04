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
        logger.debug('Timing - func:{} args:[{}, {}] took: {:.4f} sec'.format(f.__name__, args, kw, te - ts))
        return result

    return wrap
