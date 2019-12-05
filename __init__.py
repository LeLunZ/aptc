import json

import requests
import csv
from lxml import html

from Models.route import Route
from crud import *
import urllib.parse as urlparse
from urllib.parse import parse_qs

logos_set = set()


def get_location_suggestion_from_string(location):
    oebb_location = requests.get(
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
    response = requests.post("https://fahrplan.oebb.at/bin/stboard.exe/dn", data=payload,
                             params=querystring)
    tree = html.fromstring(response.content)
    all_stations = tree.xpath('//*/option/@value')
    return all_stations


def get_all_routes_from_station(station_id):
    routes_of_station = requests.get(
        'http://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_liveticker&evaId=' + str(
            int(station_id)) + '&boardType=arr&time=00:00'
                               '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate'
                               '=today&productsFilter=1011111111011')
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
    response = requests.post(url, data=payload, params=querystring)
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
        route_page = requests.get(url)
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
                print(operator)
        if extra_info_traffic_day:
            traffic_day = list(filter(lambda x: x.strip() != '', extra_info_traffic_day))
            if traffic_day or traffic_day is None:
                traffic_day = traffic_day[0].strip()
            pass
        if extra_info_remarks:
            remarks = list(filter(lambda x: x != '', map(lambda x: x.strip(), extra_info_remarks)))
            pass
        route_short_name = \
        tree.xpath('((//*/tr[@class=$first])[1]/td[@class=$second])[last()]/text()', first='zebracol-2',
                   second='center sepline')[0].strip()
        route_info = tree.xpath('//*/span[@class=$first]/img/@src', first='prodIcon')[0]
        route_type = None
        if route_info not in logos_set:
            print('if route_info == \'' + route_info + '\':')
            print(url)
            logos_set.add(route_info)

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
        new_route = Route(agency_id=new_agency.agency_id,
                          route_id=url.split('dn/')[-1],
                          route_short_name=route_short_name,
                          route_type=route_type,
                          route_url=url)
        add_route(new_route)
        for i in range(len(all_stations)):
            link = all_links_of_station[i].split('&input=')[1].split('&')[0]
            new_stop = Stop(stop_id=link, stop_name=all_stations[i],
                            stop_url=remove_param_from_url(all_links_of_station[i], '&time='))
            add_stop(new_stop)

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
                continue
            new_stop = Stop(stop_id=id, stop_name=name, location_type=stop_location_type,
                            parent_station=main_station.stop_id)
            add_stop(new_stop)
        commit()


if __name__ == "__main__":
    with open('bus_stops.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                location_data = get_location_suggestion_from_string(row[0])
                suggestion = location_data['suggestions']
                line_count += 1
                main_station = suggestion[0]
                new_stop = Stop(stop_id=str(int(main_station['extId'])), stop_name=main_station['value'],
                                stop_lat=str_to_geocord(main_station['ycoord']),
                                stop_lon=str_to_geocord(main_station['xcoord']))
                add_stop(new_stop)
                all_station_ids = get_all_station_ids_from_station(main_station)
                all_station_names = None
                if all_station_ids is None or all_station_ids:
                    all_station_ids = [main_station['extId']]
                else:
                    all_station_names = list(map(lambda x: ''.join(x.split('|')[:-1]), all_station_ids))
                    all_station_ids = list(map(lambda x: x.split('|')[-1], all_station_ids))
                public_transportation_journey = []
                save_simple_stops(all_station_names, all_station_ids, new_stop)
                for station_id in all_station_ids:
                    json_data = get_all_routes_from_station(station_id)
                    if json_data['maxJ'] is not None:
                        public_transportation_journey.extend(
                            list(map(lambda x: x, json_data['journey'])))

                routes = []
                all_transport = get_all_name_of_transport_distinct(public_transportation_journey)
                for route in all_transport:
                    routes.extend(get_all_routes_of_transport_and_station(route, main_station))
                for route in routes:
                    load_route(route)
                commit()
                print("test")

    print(f'Processed {line_count} lines.')

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
