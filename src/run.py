import logging

from Functions.config import getConfig
from Functions.helper import export_all_tables
from crawl_oebb import crawl, crawl_routes
from stop_crawler import start_stop_crawler, crawl_stops

logging.basicConfig(filename='./Data/aptc.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

if __name__ == '__main__':
    try:
        if getConfig('crawl'):
            start_stop_crawler()
            crawl_routes()
            count = 1
            while count != 0:
                print('Restarting crawling')
                logging.debug('Restarting crawling')
                crawl_stops()
                count = crawl()
    except KeyError as e:
        pass
    try:
        if getConfig('export'):
            export_all_tables()
    except KeyError:
        pass
