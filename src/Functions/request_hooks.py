import json
from urllib.parse import parse_qs, urlparse

from lxml import html

from Functions.helper import str_to_geocord
from Models.stop import Stop


def extract_real_name_from_stop_page(resp, *args, **kwargs):
    tree = html.fromstring(resp.content)
    ext_id = parse_qs(urlparse(resp.request.url).query)['input'][0]
    name = str(tree.xpath('//*/input[@name=$tag]/@value', tag='input')[0]).removesuffix(f'#{ext_id}')

    resp.data = (int(ext_id), name)


def request_stops_processing_hook(resp, *args, **kwargs):
    locations = resp.content[8:-22]
    locations = json.loads(locations.decode('iso-8859-1'))
    suggestion = locations['suggestions']
    stops = []
    for count, se in enumerate(suggestion):
        if se['type'] in ['2', '4']:
            continue  # 2 is address, 4 is POI
        if se['type'] == "1":
            if se['extId'].startswith('81'):
                if (int(se['prodClass']) & 2280) != 0:
                    pass  # its a meta object with routes. but sometimes it doesnt have children (so its not a meta anymore)
                    location_type = -1  # 1 or 0
                else:
                    pass  # its a train station. Comes with other stations which are also stations.
                    # but sometimes object is a parent station with routes and children stops
                    location_type = -1  # 0 or 1
            else:
                if (int(se['prodClass']) & 63) != 0:
                    location_type = -1  # 0 or 1
                    pass  # Its a station (metahst) (Bus, Tram, Train etc). But sometimes its also a stop
                if (int(se['prodClass']) & 63) == 0:
                    pass  # its a single stop no parent
                    location_type = 0

        if (ext_id_str := str(int(se['extId']))).startswith('11'):
            pass  # continue?? Meta Object like Cities
        elif ext_id_str.startswith('13') and (int(se['prodClass']) & 63) != 0:
            pass  # Its a station (metahst) (Bus, Tram, Train etc)
            location_type = 1
        elif ext_id_str.startswith('13') and (int(se['prodClass']) & 63) == 0:
            location_type = 1

        stops.append(Stop(stop_name=se['value'],
                          stop_lat=str_to_geocord(se['ycoord']),
                          stop_lon=str_to_geocord(se['xcoord']), input=se['value'], ext_id=int(se['extId']),
                          prod_class=int(se['prodClass']),
                          stop_url='https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input=' + str(
                              int(se['extId'])),
                          location_type=0, siblings_searched=count == 0, crawled=False, info_searched=True))
    resp.data = stops


def request_station_id_processing_hook(resp, *args, **kwargs):
    tree = html.fromstring(resp.content)
    all_stations = tree.xpath('//*/option/@value')
    if all_stations is not None and len(all_stations) > 0:
        resp.data = all_stations
    else:
        data = parse_qs(resp.request.body)
        inputRef: str = data['inputRef'][0]
        hash_sign_i = inputRef.rindex('#')
        output = f'{inputRef[:hash_sign_i]}|{inputRef[hash_sign_i + 1:]}'
        resp.data = [output]


def response_journey_hook(resp, *args, **kwargs):
    tree = html.fromstring(resp.content)
    all_routes = tree.xpath('//*/td[@class=$name]/a/@href', name='fcell')
    all_routes = list(map(lambda l: l.split('?')[0], all_routes))
    resp.data = all_routes
