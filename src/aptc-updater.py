import time
from concurrent.futures import as_completed
from typing import List

from requests_futures.sessions import FuturesSession

from Functions.oebb_requests import requests_retry_session_async
from Functions.request_hooks import request_stops_processing_hook, extract_real_name_from_stop_page
from crud import *

future_session_stops = requests_retry_session_async(session=FuturesSession())


def load_stop_info(stops: List[Stop]):
    futures = [future_session_stops.get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=10&S=' +
        stop.stop_name + '?&js=false&',
        verify=False, timeout=6, hooks={'response': request_stops_processing_hook}) for
        stop
        in stops]

    stop_ext_id_dict = {stop.stop_name: stop for stop in stops}
    stop_page_req = []
    for i in as_completed(futures):
        try:
            response = i.result()
            response_data = response.data[0]
            for count, stop in enumerate(response_data):
                if stop.ext_id in stop_ext_id_dict:
                    real_stop: Stop = stop_ext_id_dict[stop.ext_id]
                    real_stop.prod_class = stop.prod_class
                    real_stop.stop_lat = stop.stop_lat
                    real_stop.stop_lon = stop.stop_lon
                    stop_url = f'https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input={stop.ext_id}'
                    stop_page_req.append(future_session_stops.get(stop_url, verify=False,
                                                                  hooks={'response': extract_real_name_from_stop_page}))
        except Exception as e:
            pass

    for i in as_completed(stop_page_req):
        try:
            response = i.result()
            ext_id, name = response.data
            stop = stop_ext_id_dict[ext_id]
            stop.stop_name = name
        except Exception as e:
            pass

    return [stop for stop in stops if stop.prod_class is None]


if __name__ == '__main__':
    stops = get_all_stops_without_location(10)
    while True:
        stops_not_found = load_stop_info(stops)
        for stop in stops_not_found:
            stop.prod_class = -1
        commit()
        # match_station_with_google_maps(stops_not_found)
        # match_station_with_parents()
        stops = get_all_stops_without_location(10)
        if stops is None or len(stops) == 0:
            time.sleep(10)
