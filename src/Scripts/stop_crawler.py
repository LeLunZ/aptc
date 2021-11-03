import atexit
import csv
import pickle
import signal
from concurrent.futures import as_completed
from pathlib import Path
from urllib.parse import unquote

import urllib3
from requests_futures.sessions import FuturesSession

from Functions.helper import skip_stop
from Functions.oebb_requests import requests_retry_session_async
from Functions.request_hooks import request_stops_processing_hook, extract_real_name_from_stop_page, \
    request_station_id_processing_hook
from Scripts.crud import *

fiona_geometry = None
try:
    import fiona
    from shapely.geometry import shape
except (ImportError, AttributeError):
    logger.debug('Gdal not installed. Shapefile wont work')
    fiona_geometry = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stop_ids_db = set([int(e.ext_id) for e in get_all_ext_id_from_stops()])
stop_ids_searched = set([int(e.ext_id) for e in get_all_ext_id_from_stops_where_siblings_searched()])
stop_ids_info = set([int(e.ext_id) for e in get_all_ext_id_from_stops_where_info_searched()])
searched_text = set([str(e.stop_name) for e in get_all_names_from_searched_stops()])
state_path = Path('Data/pickle/state.pkl')
cur_line = 0
finished_crawling = None
file_hash = None
future_session_stops = requests_retry_session_async(session=FuturesSession())
crawlStopOptions = None
southLatBorder = None
northLatBorder = None
westLonBorder = None
eastLonBorder = None


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in stop_ids_db:
        return False

    fiona_geometry_is_avaible = fiona_geometry is not None and fiona_geometry is not False and (
            type(fiona_geometry) is list and len(fiona_geometry) > 0)

    # If no crawlstopoptions and fiona geometry is available take every stop
    if (not fiona_geometry_is_avaible and not crawlStopOptions) or (
            crawlStopOptions and southLatBorder < stop_to_check.stop_lat < northLatBorder and westLonBorder < stop_to_check.stop_lon < eastLonBorder):
        return True
    elif fiona_geometry_is_avaible:
        point = shape({'type': 'Point', 'coordinates': [stop_to_check.stop_lon, stop_to_check.stop_lat]})
        for k in fiona_geometry:
            if point.within(k):
                return True
    return False


def handle_stop_group_response(f, stop_ext_id_dict):
    try:
        result = f.result()
        all_stations = result.data  # get all station ids from response
        station_names = list(
            map(lambda x: unquote(''.join(x.split('|')[:-1]), encoding='iso-8859-1'), all_stations))
        station_ids = list(map(lambda x: int(x.split('|')[-1]), all_stations))
        main_station: Stop = stop_ext_id_dict[station_ids[0]]
        if (ext_id_str := ','.join(str(s) for s in station_ids[1:])) != '':
            main_station.group_ext_id = ext_id_str
        for s_n, s_i in zip(station_names[1:], station_ids[1:]):
            if s_i not in stop_ids_db:
                stop_ids_db.add(s_i)
                stop = Stop(stop_name=s_n, ext_id=s_i,
                            stop_url='https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input=' + str(
                                s_i),
                            location_type=0)
                add_stop_without_check(stop)
    except Exception as e:
        pass


def load_all_stops_to_crawl(stop_names):
    futures = [future_session_stops.get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=35&S=' +
        stop_name + '?&js=false&',
        verify=False, timeout=6, hooks={'response': request_stops_processing_hook}) for
        stop_name in stop_names if stop_name not in searched_text]
    searched_text.update(stop_names)
    stop_ext_id_dict = {}
    stop_page_req = []

    for i in as_completed(futures):
        try:
            response = i.result()
            response_data = response.data
            for count, stop in enumerate(response_data):
                if stop_is_to_crawl(stop):
                    if stop.siblings_searched:
                        stop_ids_searched.add(stop.ext_id)
                    stop_ids_db.add(stop.ext_id)
                    stop_ids_info.add(stop.ext_id)
                    stop_url = f'https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input={stop.ext_id}'
                    stop_ext_id_dict[stop.ext_id] = stop
                    stop_page_req.append(stop_url)
        except Exception as e:
            pass

    futures_ = [future_session_stops.get(stop_url, verify=False,
                                         hooks={'response': extract_real_name_from_stop_page}) for stop_url in
                stop_page_req]

    futures_stops_args = []
    for i in as_completed(futures_):
        try:
            response = i.result()
            ext_id, name = response.data
            stop = stop_ext_id_dict[ext_id]
            stop.stop_name = name
            add_stop_without_check(stop)
            querystring = {"ld": "3"}
            payload = {
                'sqView': '1&input=' + stop.stop_name + '&time=21:42&maxJourneys=50&dateBegin=&dateEnd=&selectDate=&productsFilter=0000111011&editStation=yes&dirInput=&',
                'input': stop.stop_name,
                'inputRef': stop.stop_name + '#' + str(stop.ext_id),
                'sqView=1&start': 'Information aufrufen',
                'productsFilter': '0000111011'
            }
            futures_stops_args.append(("https://fahrplan.oebb.at/bin/stboard.exe/dn", payload, querystring))
        except Exception as e:
            pass
    futures_ = [future_session_stops.post(url, data=payload, params=querystring, verify=False,
                                          hooks={'response': request_station_id_processing_hook}) for
                (url, payload, querystring) in futures_stops_args]

    for f in as_completed(futures_):
        handle_stop_group_response(f, stop_ext_id_dict)


def load_all_stops_to_crawl_(stops):
    def hook_factory(stop: Stop):
        def object_interceptor(response, *request_args, **request_kwargs):
            response.stop = stop
            request_stops_processing_hook(response, request_args, request_kwargs)

        return object_interceptor

    search_texts = [stop.stop_name for stop in stops]
    futures = [future_session_stops.get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=35&S=' +
        stop.stop_name + '?&js=false&',
        verify=False, timeout=6, hooks={'response': hook_factory(stop)}) for
        stop in stops]
    searched_text.update(search_texts)
    stop_ext_id_dict = {stop.ext_id: stop for stop in stops}
    stops_for_search = set()
    futures_stops_args = []
    for i in as_completed(futures):
        try:
            response = i.result()
            response_data = response.data
            real_stop = response.stop
            if real_stop.siblings_searched is False:
                stop_ids_searched.add(real_stop.ext_id)
                real_stop.siblings_searched = True
                all_stops_to_add = [stop.stop_name for stop in response_data if stop_is_to_crawl(stop)]
                stops_for_search.update(all_stops_to_add)
            if real_stop.info_searched is False:
                stop_ids_info.add(real_stop.ext_id)
                real_stop.info_searched = True
                hopeful_stop = [stop for stop in response_data if stop.ext_id == real_stop.ext_id]
                if len(hopeful_stop) != 0:
                    stop = hopeful_stop[0]
                    real_stop.stop_lat = stop.stop_lat
                    real_stop.stop_lon = stop.stop_lon
                    real_stop.input = stop.input
                    real_stop.prod_class = stop.prod_class
                querystring = {"ld": "3"}
                payload = {
                    'sqView': '1&input=' + str(
                        real_stop.ext_id) + '&time=21:42&maxJourneys=50&dateBegin=&dateEnd=&selectDate=&productsFilter=0000111011&editStation=yes&dirInput=&',
                    'input': str(real_stop.ext_id),
                    'inputRef': real_stop.stop_name + '#' + str(real_stop.ext_id),
                    'sqView=1&start': 'Information aufrufen',
                    'productsFilter': '0000111011'
                }
                futures_stops_args.append(("https://fahrplan.oebb.at/bin/stboard.exe/dn", payload, querystring))
        except Exception as e:
            pass

    futures_ = [future_session_stops.post(url, data=payload, params=querystring, verify=False,
                                          hooks={'response': request_station_id_processing_hook}) for
                (url, payload, querystring) in futures_stops_args]

    for f in as_completed(futures_):
        handle_stop_group_response(f, stop_ext_id_dict)

    if len(stops_for_search) > 0:
        load_all_stops_to_crawl(stops_for_search)


def crawl_stops(init=False):
    global cur_line, finished_crawling, file_hash
    stop_set = set()
    file_path: str = getConfig('csvFile')
    csv_path = Path(f'../Data/') / file_path
    if init and csv_path.exists():
        with open(csv_path) as csv_file:
            file_hash = xxhash.xxh3_64(''.join(csv_file.readlines())).hexdigest()
            csv_file.seek(0)
            csv_reader = csv.reader(csv_file, delimiter=',')
            row_count = sum(1 for _ in csv_reader)
            begin = 1
            end = row_count - 1
            csv_file.seek(0)

            for row in skip_stop(csv_reader, begin, end):
                stop_set.add(row[0])

    try:
        max_stops_to_crawl = getConfig('batchSize')
    except:
        max_stops_to_crawl = 3
    logger.info("starting stops crawling")
    stop_list = list(stop_set)
    if init:
        # Read State and check if we are continuing from a crash
        if state_path.exists():
            with open(state_path, 'rb') as f:
                (c_line, f_hash) = pickle.load(f)
            state_path.unlink(missing_ok=True)
            if f_hash == file_hash:
                if (cur_line := (c_line - max_stops_to_crawl)) < 0:
                    cur_line = 0
                stop_list = stop_list[c_line:]
        atexit.register(save_csv_state)
        signal.signal(signal.SIGTERM, save_csv_state)
        signal.signal(signal.SIGINT, save_csv_state)
        finished_crawling = False
        # Crawl csv file with limit
        while len(stop_list) != 0:
            to_crawl = stop_list[:max_stops_to_crawl]
            stop_list = stop_list[max_stops_to_crawl:]
            cur_line += max_stops_to_crawl
            load_all_stops_to_crawl(to_crawl)
            commit()
            logger.info(f'finished batch {cur_line}')
        finished_crawling = True
        logger.info(f'finished all from csv')
    # Crawl uncrawled stops from database
    global stop_ids_db
    stop_ids_db = set([int(e.ext_id) for e in get_all_ext_id_from_stops()])
    uncrawled = sibling_search_stops(max_stops_to_crawl)
    while len(uncrawled) != 0:
        load_all_stops_to_crawl_(uncrawled)
        for stop in uncrawled:
            stop.siblings_searched = True
            stop.info_searched = True
        commit()
        uncrawled = sibling_search_stops(max_stops_to_crawl)

    logger.info("finished stops crawling")
    # match_station_with_google_maps()
    # Call after crawling whole Routes
    # match_station_with_parents()


def save_csv_state(sig=None, frame=None):
    import pickle
    if finished_crawling is None:
        return
    if not finished_crawling and file_hash != 0:
        file = open(state_path, 'wb')
        data_f = (cur_line, file_hash)
        pickle.dump(data_f, file)
        file.close()
    else:
        state_path.unlink(missing_ok=True)


def start_stop_crawler(only_init=False):
    global northLatBorder, southLatBorder, westLonBorder, eastLonBorder, crawlStopOptions, fiona_geometry
    try:
        crawlStopOptions = 'crawlStopOptions' in getConfig()
        if fiona_geometry is not False:
            try:
                shapefile = getConfig('crawlStopOptions.shapefile')
                fiona_shape = fiona.open(shapefile)
                fiona_iteration = iter(fiona_shape)
                fiona_geometry = []
                for r in fiona_iteration:
                    fiona_geometry.append(shape(r['geometry']))
                del fiona_shape
                del fiona_iteration
            except (KeyError, FileNotFoundError, Exception):
                pass

        if crawlStopOptions:
            northLatBorder = getConfig('crawlStopOptions.northLatBorder')
            southLatBorder = getConfig('crawlStopOptions.southLatBorder')
            westLonBorder = getConfig('crawlStopOptions.westLonBorder')
            eastLonBorder = getConfig('crawlStopOptions.eastLonBorder')
    except KeyError as e:
        crawlStopOptions = False
    if not only_init:
        crawl_stops(init=True)


if __name__ == '__main__':
    start_stop_crawler()
    exit(0)
