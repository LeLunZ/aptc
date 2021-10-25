from Functions.config import getConfig
from Functions.helper import export_all_tables
from crawl_oebb import crawl, crawl_routes
from stop_crawler import start_stop_crawler, crawl_stops

if __name__ == '__main__':
    try:
        if getConfig('crawl'):
            start_stop_crawler()
            crawl_routes()
            count = 1
            while count != 0:
                crawl_stops()
                count = crawl()
    except KeyError as e:
        pass
    try:
        if getConfig('export'):
            export_all_tables()
    except KeyError:
        pass
