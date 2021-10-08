import requests
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import json
from lxml import html
from datetime import datetime

date_w = ['13.07.2020']
days_name = {
    0: 'Mo',
    1: 'Di',
    2: 'Mi',
    3: 'Do',
    4: 'Fr',
    5: 'Sa',
    6: 'So',
}
weekday_name = [days_name[datetime.strptime(date_w[0], '%d.%m.%Y').weekday()]]


def set_date(date_str):
    global date_w, weekday_name
    date_w[0] = date_str
    weekday_name[0] = days_name[datetime.strptime(date_w[0], '%d.%m.%Y').weekday()]


def requests_retry_session(
        retries=5,
        backoff_factor=0.2,
        session=None,
):
    session = session or requests.session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def requests_retry_session_async(
        retries=4,
        backoff_factor=0.4,
        session=None,
):
    session = session or FuturesSession()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_location_suggestion_from_string(location: str):
    oebb_location = requests_retry_session().get(
        'https://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' + location + '?&js=false&',
        verify=False)
    locations = oebb_location.content[8:-22]
    locations = json.loads(locations.decode('iso-8859-1'))
    return locations


def get_all_station_ids_from_station(station):
    querystring = {"ld": "3"}
    payload = {
        'sqView': '1&input=' + station[
            'value'] + '&time=21:42&maxJourneys=50&dateBegin=&dateEnd=&selectDate=&productsFilter=0000111011&editStation=yes&dirInput=&',
        'input': station['value'],
        'inputRef': station['value'] + '#' + str(int(station['extId'])),
        'sqView=1&start': 'Information aufrufen',
        'productsFilter': '0000111011'
    }
    response = requests_retry_session().post("https://fahrplan.oebb.at/bin/stboard.exe/dn", data=payload,
                                             params=querystring)
    tree = html.fromstring(response.content)
    all_stations = tree.xpath('//*/option/@value')
    return all_stations


def get_all_routes_from_station(station_id):
    try:
        routes_of_station = requests_retry_session().get(
            'https://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_scotty.vs_liveticker&evaId=' + str(
                int(station_id)) + '&boardType=arr&time=00:00'
                                   '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate'
                                   '=period&dateBegin=' + date_w[0] + 'dateEnd=' + date_w[0] + '&productsFilter=1011111111011',
            verify=False)
        json_data = json.loads(routes_of_station.content.decode('iso-8859-1')[14:-1])
    except:
        json_data = None
    return json_data


def get_all_routes_of_transport_and_station(transport_number, station):
    url = "https://fahrplan.oebb.at/bin/trainsearch.exe/dn"
    querystring = {"ld": "2"}
    payload = {
        'trainname': transport_number,
        'stationname': station['value'],
        'REQ0JourneyStopsSID': station['id'],
        'selectDate': 'oneday',
        'date': f'{weekday_name}, {date_w}',
        'wDayExt0': 'Mo|Di|Mi|Do|Fr|Sa|So',
        'periodStart': '20.04.2020',
        'periodEnd': '12.12.2020',
        'time': '',
        'maxResults': 10000,
        'stationFilter': '81,01,02,03,04,05,06,07,08,09',
        'start': 'Suchen'
    }
    response = requests_retry_session().post(url, data=payload, params=querystring, verify=False)
    tree = html.fromstring(response.content)
    all_routes = tree.xpath('//*/td[@class=$name]/a/@href', name='fcell')
    all_routes = list(map(lambda l: l.split('?')[0], all_routes))
    return all_routes
