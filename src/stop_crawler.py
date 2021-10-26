import csv
import pickle
from concurrent.futures import as_completed
from pathlib import Path

import fiona
import urllib3
from requests_futures.sessions import FuturesSession

from Functions.helper import skip_stop, match_station_with_google_maps
from Functions.oebb_requests import requests_retry_session_async
from Functions.request_hooks import request_stops_processing_hook, extract_real_name_from_stop_page
from crud import *

from shapely.geometry import shape

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stop_ids = set([e.ext_id for e in get_all_ext_id_from_stops()])
state_path = Path('Data/state.pkl')
cur_line = 0
finished_crawling = None
file_hash = None
future_session_stops = requests_retry_session_async(session=FuturesSession())
crawlStopOptions = None
fiona_geometry = None
southLatBorder = None
northLatBorder = None
westLonBorder = None
eastLonBorder = None


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in stop_ids:
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


def load_all_stops_to_crawl(stop_names, database=False, db_stop_ids=None):
    futures = [future_session_stops.get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' +
        stop_name.split('(')[0].strip() + '?&js=false&',
        verify=False, timeout=6, hooks={'response': request_stops_processing_hook}) for
        stop_name
        in stop_names]

    stop_ext_id_dict = {}
    stop_page_req = []

    for i in as_completed(futures):
        try:
            response = i.result()
            response_data = response.data
            for stop in response_data:
                if stop_is_to_crawl(stop) or (database and stop.ext_id in db_stop_ids and stop.ext_id not in stop_ids):
                    stop_ids.add(stop.ext_id)
                    stop_url = f'https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input={stop.ext_id}'
                    stop_page_req.append(future_session_stops.get(stop_url, verify=False,
                                                                  hooks={'response': extract_real_name_from_stop_page}))
                    stop_ext_id_dict[stop.ext_id] = stop
        except Exception as e:
            pass

    for i in as_completed(stop_page_req):
        try:
            response = i.result()
            ext_id, name = response.data
            stop = stop_ext_id_dict[ext_id]
            stop.stop_name = name
            if database:
                lat = stop.stop_lat
                lon = stop.stop_lon
                stop = add_stop(stop)
                stop.location_type = 0
                stop.stop_lat = lat
                stop.stop_lon = lon
                stop.parent_station = None
            else:
                add_stop_without_check(stop)
        except Exception as e:
            pass


def crawl_stops(init=False):
    global cur_line, finished_crawling, file_hash
    stop_set = set()
    if init:
        with open('Data/bus_stops.csv') as csv_file:
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

    stop_list = list(stop_set)
    if init:
        # Read State and check if we are continuing from a crash
        if state_path.exists():
            with open(state_path, 'rb') as f:
                (c_line, f_hash) = pickle.load(f)
            state_path.unlink(missing_ok=True)
            if f_hash == file_hash:
                if cur_line := (c_line - max_stops_to_crawl) < 0:
                    cur_line = 0
                stop_list = stop_list[c_line:]

        # Crawl csv file with limit
        while len(stop_list) != 0:
            to_crawl = stop_list[:max_stops_to_crawl]
            stop_list = stop_list[max_stops_to_crawl:]
            cur_line += max_stops_to_crawl
            load_all_stops_to_crawl(to_crawl)
            commit()

    # Crawl uncrawled stops from database
    uncrawled = sibling_search_stops(max_stops_to_crawl)
    while len(uncrawled) != 0:
        stop_names = []
        cur_stop_ids = []
        for stop in uncrawled:
            stop.siblings_searched = True
            stop_names.append(stop.stop_name)
            cur_stop_ids.append(stop.ext_id)
        commit()
        load_all_stops_to_crawl(stop_names, not init, cur_stop_ids)
        commit()
        uncrawled = sibling_search_stops(max_stops_to_crawl)

    match_station_with_google_maps()

    # Call after crawling whole Routes
    # match_station_with_parents()


def save_csv_state():
    import pickle
    if not finished_crawling and file_hash != 0:
        file = open(state_path, 'wb')
        data_f = (cur_line, file_hash)
        pickle.dump(data_f, file)
        file.close()
    else:
        state_path.unlink(missing_ok=True)


def start_stop_crawler():
    global northLatBorder, southLatBorder, westLonBorder, eastLonBorder, crawlStopOptions, fiona_geometry
    try:
        fiona_geometry = False
        crawlStopOptions = 'crawlStopOptions' in getConfig()
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
    crawl_stops(init=True)


if __name__ == '__main__':
    start_stop_crawler()
