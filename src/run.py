import logging
import sys
from logging.config import dictConfig

import urllib3

from Functions.config import getConfig
from Functions.helper import export_all_tables
from Scripts.crawl_oebb import crawl, crawl_routes
from Scripts.stop_crawler import start_stop_crawler, crawl_stops
from constants import logs_path

loggers = ['timed', 'console', 'failed-url']


class LogFilter(logging.Filter):
    def __init__(self, params=None, allow=False):
        super(LogFilter, self).__init__()
        self.params = params
        self.allow = allow

    def filter(self, record):
        if self.params is None:
            return True

        for param in self.params:
            if param in record.msg:
                if self.allow:
                    return True
                if not self.allow:
                    return False

        if self.allow:
            return False
        return True


dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'filters': {
        'DisallowFilter': {
            '()': LogFilter,
            'params': ['TripAlreadyPresentError', 'CalendarDataNotFoundError', 'Timing - func', 'Retrying Url'],
            'allow': False
        },
        'AllowExceptionFilter': {
            '()': LogFilter,
            'params': ['TripAlreadyPresentError', 'CalendarDataNotFoundError'],
            'allow': True
        },
        'AllowTimingFilter': {
            '()': LogFilter,
            'params': ['Timing - func'],
            'allow': True
        },
        'AllowRetryFilter': {
            '()': LogFilter,
            'params': ['Retrying Url'],
            'allow': True
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'stream': sys.stdout,
            'formatter': 'default',
            'filters': ['DisallowExceptionFilter']

        },
        'timed': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'default',
            'backupCount': 3,
            'filename': f'{str(logs_path)}/aptc.log',
            'when': 'W0',
            'filters': ['DisallowExceptionFilter']
        },
        'failed-url': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'default',
            'backupCount': 3,
            'filename': f'{str(logs_path)}/failed-urls.log',
            'when': 'W0',
            'filters': ['AllowExceptionFilter']
        },
        'timings': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'default',
            'backupCount': 3,
            'filename': f'{str(logs_path)}/function-timings.log',
            'when': 'W0',
            'filters': ['AllowTimingFilter']
        }
        'retry': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'default',
            'backupCount': 3,
            'filename': f'{str(logs_path)}/retry-urls.log',
            'when': 'W0',
            'filters': ['AllowRetryFilter']
        }
    },
    'loggers': {
        'Scripts': {
            'handlers': loggers,
            'level': 'DEBUG',
            'propagate': False
        },
        'Functions': {
            'handlers': loggers,
            'level': 'DEBUG',
            'propagate': False
        },
        'Classes': {
            'handlers': loggers,
            'level': 'DEBUG',
            'propagate': False
        },
        '__main__': {
            'handlers': loggers,
            'level': 'DEBUG',
            'propagate': False
        },
        '': {
            'handlers': loggers,
            'level': 'ERROR',
            'propagate': False
        }
    }
})

logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if __name__ == '__main__':
    try:
        if getConfig('crawl'):
            skip_csv = False
            try:
                skip_csv = getConfig('skipCSV')
            except Exception:
                skip_csv = False
            finally:
                start_stop_crawler(skip_csv)
            crawl_routes()
            count = 1
            while count != 0:
                print('Restarting crawling')
                logger.info('Restarting crawling')
                crawl_stops()
                count = crawl()
    except KeyError as e:
        logger.debug('Not crawling - no key in csv found')
    try:
        if getConfig('export'):
            export_all_tables()
    except KeyError:
        logger.debug('Not Export - no key in csv found')
