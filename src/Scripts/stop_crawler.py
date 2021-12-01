import atexit
import csv
import errno
import os
import pickle
import signal
from concurrent.futures import as_completed
from pathlib import Path
from urllib.parse import unquote

import urllib3
from requests_futures.sessions import FuturesSession

from Functions.geometry import stop_is_to_crawl_geometry
from Functions.helper import skip_stop, match_station_with_google_maps
from Functions.oebb_requests import requests_retry_session_async
from Functions.request_hooks import request_stops_processing_hook, extract_real_name_from_stop_page, \
    request_station_id_processing_hook
from Scripts.crud import *
from constants import data_path, state_path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stop_ids_db = set([int(e.ext_id) for e in get_all_ext_id_from_stops()])
# stop_ids_info = set([int(e.ext_id) for e in get_all_ext_id_from_stops_where_info_searched()])
searched_text = set([str(e.stop_name) for e in get_all_names_from_searched_stops()])
cur_line = 0
finished_crawling = None
file_hash = None
future_session_stops = requests_retry_session_async(session=FuturesSession())


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in stop_ids_db:
        return False

    return stop_is_to_crawl_geometry(stop_to_check)


def handle_stop_group_response(f, stop_ext_id_dict):
    try:
        result = f.result()
        all_stations = result.data  # get all station ids from response
        station_names = list(
            map(lambda x: unquote(''.join(x.split('|')[:-1]), encoding='iso-8859-1'), all_stations))
        station_ids = list(map(lambda x: int(x.split('|')[-1]), all_stations))
        main_station: Stop = stop_ext_id_dict[station_ids[0]]
        if (ext_id_str := ','.join(str(s) for s in sorted(station_ids[:]))) != '':
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
                    stop_ids_db.add(stop.ext_id)
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
                real_stop.siblings_searched = True
                all_stops_to_add = [stop.stop_name for stop in response_data if stop_is_to_crawl(stop)]
                stops_for_search.update(all_stops_to_add)
            if real_stop.info_searched is False:
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
    if init:
        try:
            file_path: str = getConfig('csvFile')
        except KeyError:
            pass
        else:
            csv_path = data_path / file_path
            if not csv_path.exists():
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(csv_path))

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
    except KeyError:
        max_stops_to_crawl = 3
    logger.info("starting stops crawling")
    if init:
        # Read State and check if we are continuing from a crash
        stop_list = list(stop_set)
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
    count = 0
    while len(uncrawled) != 0:
        load_all_stops_to_crawl_(uncrawled)
        for stop in uncrawled:
            stop.siblings_searched = True
            stop.info_searched = True
        commit()
        count += len(uncrawled)
        logger.info(f'finished stop batch {count}')
        uncrawled = sibling_search_stops(max_stops_to_crawl)

    logger.info("finished stops crawling")
    logger.info('starting google stop search')
    match_station_with_google_maps()
    logger.info('google stop search finished')
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
    # Here you can do some init stuff

    if not only_init:
        crawl_stops(init=True)


if __name__ == '__main__':
    start_stop_crawler()
    exit(0)
