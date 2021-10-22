import csv
import json
import pickle
from concurrent.futures import as_completed
from pathlib import Path

import fiona
import urllib3
import xxhash
from requests_futures.sessions import FuturesSession
from shapely.geometry import shape

from crud import *
from Functions.config import getConfig
from Functions.helper import str_to_geocord, skip_stop
from Functions.oebb_requests import requests_retry_session_async, date_w
from Models.stop import Stop

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stop_ids = set()
state_path = Path('Data/state.pkl')
cur_line = 0
finished_crawling = None
file_hash = None


def request_stops_processing_hook(resp, *args, **kwargs):
    locations = resp.content[8:-22]
    locations = json.loads(locations.decode('iso-8859-1'))
    suggestion = locations['suggestions']
    stops = []
    for se in suggestion:
        if se['type'] in ['2', '4']:
            continue  # 2 is address, 4 is POI
        if se['type'] == "1":
            if se['extId'].startswith('81'):
                if (int(se['prodClass']) & 2280) != 0:
                    pass  # Its a station (metahst) (Bus, Tram, Train etc)
                else:
                    pass  # its a train station
            else:
                if (int(se['prodClass']) & 63) != 0:
                    pass  # Its a station (metahst) (Bus, Tram, Train etc)

        if (ext_id_str := str(int(se['extId']))).startswith('11'):
            continue  # Meta Object
        elif ext_id_str.startswith('13') and int(se['prodClass']) & 63 != 0:
            pass  # Its a station (metahst) (Bus, Tram, Train etc)

        stops.append(Stop(stop_name=se['value'],
                          stop_lat=str_to_geocord(se['ycoord']),
                          stop_lon=str_to_geocord(se['xcoord']), input=se['value'], ext_id=int(se['extId']),
                          prod_class=int(se['prodClass']),
                          location_type=0))
    resp.data = stops


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in stop_ids:
        return False

    fiona_geometry_is_avaible = fiona_geometry is not None and fiona_geometry is not False and (
            type(fiona_geometry) is list and len(fiona_geometry) > 0)

    if (not fiona_geometry_is_avaible and not crawlStopOptions) or (
            crawlStopOptions and southLatBorder < stop_to_check.stop_lat < northLatBorder and westLonBorder < stop_to_check.stop_lon < eastLonBorder):
        return True
    elif fiona_geometry_is_avaible:
        point = shape({'type': 'Point', 'coordinates': [stop_to_check.stop_lon, stop_to_check.stop_lat]})
        for k in fiona_geometry:
            if point.within(k):
                return True
    return False


def load_all_stops_to_crawl(stop_names):
    future_session_stops = requests_retry_session_async(session=FuturesSession())
    futures = [future_session_stops.get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' +
        stop_name.split('(')[0].strip() + '?&js=false&',
        verify=False, timeout=6, hooks={'response': request_stops_processing_hook}) for
        stop_name
        in stop_names]

    for i in as_completed(futures):
        try:
            response = i.result()
            response_data = response.data
            for stop in response_data:
                if stop_is_to_crawl(stop):
                    stop_ids.add(stop.ext_id)
                    add_stop_without_check(stop)
        except Exception as e:
            pass


def crawl_stops():
    global cur_line, finished_crawling, file_hash
    with open('Data/bus_stops.csv') as csv_file:
        file_hash = xxhash.xxh3_64(''.join(csv_file.readlines())).hexdigest()
        csv_file.seek(0)
        csv_reader = csv.reader(csv_file, delimiter=',')
        row_count = sum(1 for _ in csv_reader)
        begin = 1
        end = row_count - 1
        csv_file.seek(0)
        stop_set = set()
        for row in skip_stop(csv_reader, begin, end):
            stop_set.add(row[0])
    try:
        max_stops_to_crawl = getConfig('batchSize')
    except:
        max_stops_to_crawl = 3

    stop_list = list(stop_set)
    # Read State and check if we are continuing from a crash
    if state_path.exists():
        with open(state_path, 'rb') as f:
            (c_line, f_hash) = pickle.load(f)
        state_path.unlink(missing_ok=True)
        if f_hash == file_hash:
            if cur_line := (c_line - max_stops_to_crawl) < 0:
                cur_line = 0
            stop_list = stop_list[c_line:]

    while len(stop_list) != 0:
        to_crawl = stop_list[:max_stops_to_crawl]
        stop_list = stop_list[max_stops_to_crawl:]
        cur_line += max_stops_to_crawl
        load_all_stops_to_crawl(to_crawl)
        commit()


if __name__ == "__main__":
    stops_ext_id = set([e.ext_id for e in get_all_ext_id_from_stops()])
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
    crawl_stops()
exit(0)
