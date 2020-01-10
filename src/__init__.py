import json
import logging
from Models.agency import Agency
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip

from Models.calendar import Calendar

from Models.frequencies import Frequency
from Models.shape import Shape
from Models.transfers import Transfer
from itertools import islice

import requests
import csv
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import os
from zipfile import ZipFile
from crud import *

import urllib.parse as urlparse
from urllib.parse import parse_qs

# TODO CSV export

logging.basicConfig(filename='./aptc.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

service_months = {
    'Jan': 1,
    'Feb': 2,
    'Mär': 3,
    'Apr': 4,
    'Mai': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Okt': 10,
    'Nov': 11,
    'Dez': 12
}


def requests_retry_session(
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_location_suggestion_from_string(location):
    oebb_location = requests_retry_session().get(
        'http://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' + location + '?&js=false&')
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
    date = '07.10.2019'
    routes_of_station = requests_retry_session().get(
        'http://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_scotty.vs_liveticker&evaId=' + str(
            int(station_id)) + '&boardType=arr&time=00:00'
                               '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate'
                               '=period&dateBegin=' + date + 'dateEnd=' + date + '&productsFilter=1011111111011')
    json_data = json.loads(routes_of_station.content.decode('iso-8859-1')[14:-1])
    return json_data


def get_all_routes_of_transport_and_station(transport_number, station):
    url = "http://fahrplan.oebb.at/bin/trainsearch.exe/dn"
    querystring = {"ld": "2"}
    payload = {
        'trainname': transport_number,
        'stationname': station['value'],
        'REQ0JourneyStopsSID': station['id'],
        'selectDate': 'oneday',
        'date': "Do, 21.11.2019",
        'wDayExt0': 'Mo|Di|Mi|Do|Fr|Sa|So',
        'periodStart': '15.09.2019',
        'periodEnd': '12.12.2020',
        'time': '',
        'maxResults': 10000,
        'stationFilter': '81,01,02,03,04,05,06,07,08,09',
        'start': 'Suchen'
    }
    response = requests_retry_session().post(url, data=payload, params=querystring)
    tree = html.fromstring(response.content)
    all_routes = tree.xpath('//*/td[@class=$name]/a/@href', name='fcell')
    return all_routes


def get_all_name_of_transport_distinct(list_of_transport):
    all_transport = set()
    for transport in list_of_transport:
        if transport['pr'] not in all_transport:
            all_transport.add(transport['pr'])
    return list(all_transport)


def remove_param_from_url(url, to_remove):
    splitted_url = url.split(to_remove)
    left_part = splitted_url[0]

    right_part = '&'.join(list(filter(lambda x: x.strip() != '', splitted_url[1].split('&')[::-1][:-1])))
    return left_part + '&' + right_part


def load_route(url):
    url = url.split('?')[0]
    if not route_exist(url):
        route_page = requests_retry_session().get(url)
        tree = html.fromstring(route_page.content)
        all_stations = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/text()', first='zebracol-2',
                                  second="zebracol-1")
        all_links_of_station = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/@href', first='zebracol-2',
                                          second="zebracol-1")
        extra_info_operator = tree.xpath('//*/strong[text() =$first]/../text()', first='Betreiber:')
        extra_info_traffic_day = tree.xpath('//*/strong[text() =$first]/../text()', first='Verkehrstage:')
        extra_info_remarks = tree.xpath('//*/strong[text() =$first]/../text()', first='Bemerkungen:')
        new_agency = Agency()
        if extra_info_operator:
            operator = list(filter(lambda x: x.strip() != '', extra_info_operator))[0].strip()
            try:
                agency = operator.split(', ')
                if len(agency) is 1:
                    agency_phone = None
                    agency_name = ','.join(agency[0].split(',')[:-1])
                else:
                    agency_name = agency[0]
                    agency_phone = agency[1]
                new_agency.agency_id = agency_name
                new_agency.agency_phone = agency_phone
                add_agency(new_agency)
            except:
                pass
        else:
            new_agency = get_from_table_first(Agency)
        if extra_info_traffic_day:
            traffic_day = list(filter(lambda x: x.strip() != '', extra_info_traffic_day))
            if traffic_day:
                traffic_day = traffic_day[0].strip()
            pass
        if extra_info_remarks:
            remarks = list(filter(lambda x: x != '', map(lambda x: x.strip(), extra_info_remarks)))
            pass
        route_short_name = \
            tree.xpath('((//*/tr[@class=$first])[1]/td[@class=$second])[last()]/text()', first='zebracol-2',
                       second='center sepline')[0].strip()
        if ' ' in route_short_name:
            route_long_name = route_short_name
            route_short_name = route_short_name.split(' ')[1]
        route_info = tree.xpath('//*/span[@class=$first]/img/@src', first='prodIcon')[0]
        route_type = None
        if route_info == '/img/vs_oebb/rex_pic.gif':
            route_type = 2  # Regional zug
        elif route_info == '/img/vs_oebb/r_pic.gif':
            route_type = 2
        elif route_info == '/img/vs_oebb/s_pic.gif':
            route_type = 1
        elif route_info == '/img/vs_oebb/os_pic.gif':
            route_type = 2
        elif route_info == '/img/vs_oebb/ex_pic.gif':
            route_type = 2
        elif route_info == '/img/vs_oebb/hog_pic.gif':
            route_type = 3
        elif route_info == '/img/vs_oebb/nmg_pic.gif':
            route_type = 3
        elif route_info == '/img/vs_oebb/ntr_pic.gif':
            route_type = 0
        elif route_info == '/img/vs_oebb/hmp_pic.gif':
            route_type = 3

        new_route = Route(agency_id=new_agency.agency_id,
                          route_id=url.split('dn/')[-1],
                          route_short_name=route_short_name,
                          route_long_name=route_long_name,
                          route_type=route_type,
                          route_url=url)
        route = add_route(new_route)
        try:
            if not extra_info_traffic_day:
                raise Exception()
            if isinstance(traffic_day, list):
                logging.error(f'traffic day is list {traffic_day}')
                raise Exception()
            calendar = Calendar(service_id=route.route_id)
            weekdays = []
            official_weekdays = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
            splited_traffic_day = str(traffic_day).replace(';', '')
            splited_traffic_day = splited_traffic_day.split(' ')
            for i in official_weekdays:
                if i in splited_traffic_day:
                    weekdays.append([i, official_weekdays.index(i)])

            weekdays.sort(key=lambda x: x[1])
            if (len(weekdays) > 1 and f'{weekdays[0][0]} - {weekdays[1][0]}' in traffic_day) or len(weekdays) is 0:
                if len(weekdays) is 0:
                    weekdays[0][1] = 0
                    weekdays[1][1] = 6
                if weekdays[1][1] >= 0 >= weekdays[0][1]:
                    calendar.monday = True
                else:
                    calendar.monday = False
                if weekdays[1][1] >= 1 >= weekdays[0][1]:
                    calendar.tuesday = True
                else:
                    calendar.tuesday = False
                if weekdays[1][1] >= 2 >= weekdays[0][1]:
                    calendar.wednesday = True
                else:
                    calendar.wednesday = False
                if weekdays[1][1] >= 3 >= weekdays[0][1]:
                    calendar.thursday = True
                else:
                    calendar.thursday = False
                if weekdays[1][1] >= 4 >= weekdays[0][1]:
                    calendar.friday = True
                else:
                    calendar.friday = False
                if weekdays[1][1] >= 5 >= weekdays[0][1]:
                    calendar.saturday = True
                else:
                    calendar.saturday = False
                if weekdays[1][1] >= 6 >= weekdays[0][1]:
                    calendar.sunday = True
                else:
                    calendar.sunday = False
            else:
                logging.error(f'traffic day no valid weekdays {traffic_day}')
                raise Exception()
            if 'bis ' not in traffic_day:
                logging.error(f'traffic day no bis found {traffic_day}')
                raise Exception()
            service_info = ''.join(traffic_day.split('bis ')[1:4]).split(' ')
            day = service_info[0].replace('.', '')
            month = service_months[service_info[1]]
            year = service_info[2]
            if len(day) is 1:
                day = f'0{day}'
            if month < 10:
                month = f'0{month}'

            end_date = int(f'{year}{month}{day}')
            calendar.end_date = end_date
            service_info = ''.join(traffic_day.split('am ')[1:3]).split(' ')
            day = service_info[0].replace('.', '')
            if service_info[1] != 'bis':
                month = service_months[service_info[1]]
            if len(day) is 1:
                day = f'0{day}'
            if month < 10:
                month = f'0{month}'
            start_date = f'{year}{month}{day}'
            calendar.start_date = int(start_date)
            calendar = add_calendar(calendar)
        except Exception as e:
            class FakeCalendar:
                def __init__(self):
                    self.service_id = None

            calendar = FakeCalendar()
        for i in range(len(all_stations)):
            all_times = list(map(lambda x: x.strip(),
                                 tree.xpath('//*/tr[@class=$first or @class=$second][$count]/td[@class=$third]/text()',
                                            first='zebracol-2',
                                            second="zebracol-1", third='center sepline', count=i + 1)))
            link = all_links_of_station[i].split('&input=')[1].split('&')[0]
            new_stop = Stop(stop_id=link, stop_name=all_stations[i],
                            stop_url=remove_param_from_url(all_links_of_station[i], '&time='), location_type=0)
            stop = add_stop(new_stop)
            stop.location_type = 0
            stop.parent_station = None
            new_trip = Trip(route_id=route.route_id, service_id=calendar.service_id)
            trip: Trip = add_trip(new_trip)
            commit()
            new_stop_time = StopTime(stop_id=stop.stop_id, trip_id=trip.trip_id,
                                     arrival_time=all_times[0] if all_times[0] == '' else all_times[0] + ':00',
                                     departure_time=all_times[1] if all_times[1] == '' else all_times[1] + ':00',
                                     stop_sequence=i + 1, pickup_type=0, drop_off_type=0)
            add_stop_time(new_stop_time)
        pass


def str_to_geocord(cord: str):
    return float(cord[:2] + '.' + cord[2:])


def save_simple_stops(names, ids, main_station):
    if len(ids) > 1:
        for index, tup in enumerate(zip(names, ids)):
            name = tup[0]
            id = tup[1]
            stop_location_type = 0
            if index is 0:
                main_station.location_type = 1
                main_station.parent_station = None
                continue
            new_stop = Stop(stop_id=id, stop_name=name, location_type=stop_location_type,
                            parent_station=main_station.stop_id)
            add_stop(new_stop)
        commit()


def skip_stop(seq, begin, end):
    for i, item in enumerate(seq):
        if begin <= i <= end:
            yield item


def export_all_tables():
    tables = [Agency, Calendar, Frequency, Route, Shape, Stop, StopTime, Transfer, Trip]

    file_names = []
    os.chdir('./db')
    for i in tables:
        file_names.append(f'./{i.__table__.name}.txt')
        with open(f'./{i.__table__.name}.txt', 'w') as outfile:
            outcsv = csv.writer(outfile, delimiter=',')
            records = get_from_table(i)
            outcsv.writerow(i.firstline())
            [outcsv.writerow(row.tocsv()) for row in records]

    with ZipFile('./Archiv.zip', 'w') as zip:
        for file in file_names:
            zip.write(file)

if __name__ == "__main__":
    try:
        if os.environ['export']:
            export_all_tables()
            exit()
    except KeyError:
        pass

    with open('bus_stops.csv') as csv_file:
        error = False


        def log_error(msg):
            if not error:
                logging.error(msg)


        csv_reader = csv.reader(csv_file, delimiter=',')
        row_count = sum(1 for row in csv_reader)
        try:
            begin = int(os.environ['csvbegin']) - 1
        except KeyError:
            begin = 1
        try:
            end = int(os.environ['csvend']) - 1
        except KeyError:
            end = row_count - 1
        csv_file.seek(0)
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in skip_stop(csv_reader, begin, end):
            try:
                location_data = get_location_suggestion_from_string(row[0])
                suggestion = location_data['suggestions']
                main_station = suggestion[0]
                try:
                    new_stop = Stop(stop_id=str(int(main_station['extId'])), stop_name=main_station['value'],
                                    stop_lat=str_to_geocord(main_station['ycoord']),
                                    stop_lon=str_to_geocord(main_station['xcoord']))
                    new_stop = add_stop(new_stop)
                    all_station_ids = get_all_station_ids_from_station(main_station)
                    all_station_names = None
                    if all_station_ids is None or not all_station_ids:
                        all_station_ids = [main_station['extId']]
                    else:
                        all_station_names = list(map(lambda x: ''.join(x.split('|')[:-1]), all_station_ids))
                        all_station_ids = list(map(lambda x: x.split('|')[-1], all_station_ids))
                    public_transportation_journey = []
                    save_simple_stops(all_station_names, all_station_ids, new_stop)
                    for station_id in all_station_ids:
                        json_data = None
                        count = 0
                        while (json_data is None or json_data['maxJ'] is None) and count < 4:
                            json_data = get_all_routes_from_station(station_id)
                            count += 1
                        if json_data['maxJ'] is not None:
                            public_transportation_journey.extend(
                                list(map(lambda x: x, json_data['journey'])))

                    routes = []
                    try:
                        all_transport = get_all_name_of_transport_distinct(public_transportation_journey)
                        for route in all_transport:
                            try:
                                routes.extend(get_all_routes_of_transport_and_station(route, main_station))
                            except Exception as e:
                                logging.error(f'get_all_routes_of_transport_and_station {route} {main_station} {str(e)}')
                        for route in routes:
                            try:
                                load_route(route)
                            except:
                                logging.error(f'load_route {route}')
                        commit()
                    except Exception as e:
                        log_error(f'get_all_name_of_transport_distinct {public_transportation_journey} {str(e)}')
                except Exception as e:
                    logging.error(f"get_all_station_ids_from_station {main_station} {str(e)}")
            except Exception as e:
                logging.error(f'get_location_suggestion_from_string {row} {str(e)}')
            logging.debug(f"finished {row}")

exit(0)
# while True:
#     re = requests.get("http://fahrplan.oebb.at/bin/stboard.exe/dn?ld=3&L=vs_postbus&")
#     if re.status_code != 200:
#         print(re.status_code)
#     print(re.status_code)

# First Get request for stop
# http://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=Gallneukirchen%20Einsatz?&js=true&

# With id from 1.
# http://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_liveticker&evaId=491001&boardType=arr&time=00:00&additionalTime=0&
# disableEquivs=yes&maxJourneys=500&outputMode=tickerDataOnly&start=yes&selectDate=today

# Search for Öffi
# http://fahrplan.oebb.at/bin/trainsearch.exe/dn?ld=2&
# trainname=&stationname=Gallneukirchen+Rammesberg&REQ0JourneyStopsSID=A%3D1%40O%3DGallneukirchen+Rammesberg%40X%3D14414769%40Y%3D48362613%40U%3D181%40L%3D000416304%40B%3D1%40p%3D1573738453%40&selectDate=oneday&date=So%2C+17.11.2019&wDayExt0=Mo%7CDi%7CMi%7CDo%7CFr%7CSa%7CSo&periodStart=15.09.2019&periodEnd=12.12.2020&time=&maxResults=10&stationFilter=81%2C01%2C02%2C03%2C04%2C05%2C06%2C07%2C08%2C09&start=Suchen

# Select Öffi and save

# Search
# http://fahrplan.oebb.at/bin/trainsearch.exe/dn?ld=21&L=vs_postbus&
#  REQ0JourneyStopsSID=A=1@O=Gallneukirchen%20Einsatzzentrum@X=14412657@Y=48350945@U=181@L=000416096@B=1@p=1573738453@&
#  date=Mo,%2018.11.19&maxResults=10&selectDate=oneday&start=Suchen&stationFilter=81,01,02,03,04,05,06,07,08,09&
#  stationname=Gallneukirchen%20Einsatzzentrum&time=&trainname=

# http://fahrplan.oebb.at/bin/stboard.exe/dn?ld=21&
# sqView: 1&input=Gallneukirchen Rammesberg%23416304&time=20:27&maxJourneys=20&dateBegin=&dateEnd=&selectDate=&productsFilter=1011111111011&editStation=yes&dirInput=&
# input: Gallneukirchen Marktplatz
# REQ0JourneyStopsSID: A=1@O=Gallneukirchen Marktplatz@X=14415848@Y=48352905@U=181@L=000416064@B=1@p=1573738453@
# inputRef: Gallneukirchen Rammesberg#416304
# sqView=1&start: Information aufrufen
# productsFilter: 1011111111011

#
