import copy
import datetime
import json
import logging
import pickle
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Thread
from typing import List
from urllib.parse import parse_qs, urlparse

from lxml import html
from requests_futures.sessions import FuturesSession
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.exceptions import ReadTimeoutError, MaxRetryError

from Classes.DTOs import PageDTO
from Classes.exceptions import TripAlreadyPresentError, CalendarDataNotFoundError
from Functions.geometry import stop_is_to_crawl_geometry
from Models.agency import Agency
from Models.calendar import Calendar
from Models.calendar_date import CalendarDate
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip
from Classes.oebb_date import OebbDate, service_months, begin_date, end_date, get_std_date
from Functions.helper import add_day_to_calendar, extract_date_from_date_arr, merge_date, add_days_to_calendar, \
    get_all_name_of_transport_distinct, extract_date_objects_from_str
from Functions.oebb_requests import requests_retry_session_async, date_w, set_date, weekday_name
from Functions.request_hooks import response_journey_hook
from Functions.config import getConfig
from Scripts.crud import add_stop, add_stop_times, get_all_ext_id_from_crawled_stops, get_from_table_first, add_route, \
    add_agency, add_calendar_dates, add_trip, add_stop_without_check, get_all_stops_in_list, \
    commit, get_from_table, new_session, load_all_uncrawled_stops, add_transport_name
from constants import pickle_path, chrome_driver_path

logger = logging.getLogger(__name__)

finishUp = False

import os

q = []
oebb_img_prefix = ['/img/vs_oebb/', '/hafas-res/img/vs_oebb/']

# Route_Types
# 0 - Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.
# 1 - Subway, Metro. Any underground rail system within a metropolitan area.
# 2 - Rail. Used for intercity or long-distance travel.
# 3 - Bus. Used for short- and long-distance bus routes.
# 4 - Ferry. Used for short- and long-distance boat service.
# 5 - Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco.
# 6 - Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.
# 7 - Funicular. Any rail system designed for steep inclines.
# 11 - Trolleybus. Electric buses that draw power from overhead wires using poles.
# 12 - Monorail. Railway in which the track consists of a single rail or a beam.
# 101 - High Speed Rail Service
# 102 - Long Distance Trains
# 106 - Regional Rail Service (TER (FR), Regionalzug (DE))
# 107 - Tourist Railway Service
# 109 - Suburban Railway (S-Bahn (DE), RER (FR), S-tog (Kopenhagen))
# 115 - Vehicle Transport Rail Service
# 715 - Demand and Response Bus Service
# 1000 - Water Transport Service
# 1500 - Taxi Service
# 1700 - Miscellaneous Service
route_types = {
    'rex_pic.gif': 2,
    'r_pic.gif': 2,
    's_pic.gif': 1,
    'os_pic.gif': 2,
    'ex_pic.gif': 2,
    'hog_pic.gif': 3,
    'nmg_pic.gif': 3,
    'ntr_pic.gif': 0,
    'hmp_pic.gif': 3,
    'bus_pic.gif': 3,
    'str_pic.gif': 0,
    'er_pic.gif': 2,
    'en_pic.gif': 2,
    'atb_pic.gif': 115,
    'arz_pic.gif': 115,
    'ice_pic.gif': 101,
    'd_pic.gif': 2,
    'ast_pic.gif': 1500,
    'rb_pic.gif': 2,
    'bsv_pic.gif': 3,
    'cat_pic.gif': 1,
    'rj_pic.gif': 2,
    'nj_pic.gif': 2,
    'ec_pic.gif': 102,
    'ic_pic.gif': 102,
    'obu_pic.gif': 11,
    'icb_pic.gif': 3,
    'dps_pic.gif': 2,
    'u_pic.gif': 1,
    'wb_pic.gif': 2,
    'rjx_pic.gif': 2,
    'cjx_pic.gif': 2,
    't84_pic.gif': 1700,
    'x70_pic.gif': 1700,
    'u70_pic.gif': 1700,
    'sb_pic.gif': 6,
    'lkb_pic.gif': 0,
    'rer_pic.gif': 109,
    'sch_pic.gif': 1000,
    'rgj_pic.gif': 2,
    'val_pic.gif': 3,
    'rfb_pic.gif': 715,
    'lil_pic.gif': 107,
    'bmz_pic.gif': 107,
    'ter_pic.gif': 106,
    'dpn_pic.gif': 106,
    'sp_pic.gif': 1,
    'ssb_pic.gif': 7,
    'ecb_pic.gif': 102,
    're_pic.gif': 106,
    'zug_pic.gif': 2
}
date_arr = []
stop_times_executor = None
stop_times_to_add = []
crawled_stop_ids = set([e.ext_id for e in get_all_ext_id_from_crawled_stops()])

allg_feiertage = []


def extract_date_from_str(calendar: Calendar, date_str: str):
    official_weekdays = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    date_str = date_str.replace(';', '')
    working_dates = date_str
    irregular_dates = None
    extended_dates = None
    days_done = False
    has_fixed_date = False
    if 'nicht' in date_str:
        working_dates = date_str.split('nicht')[0]
        irregular_dates = ' '.join(date_str.split('nicht')[1:])
        if 'auch' in irregular_dates:
            extended_dates = irregular_dates.split('auch')[1]
            irregular_dates = irregular_dates.split('auch')[0]
    elif 'auch' in date_str:
        working_dates = date_str.split('auch')[0]
        extended_dates = date_str.split('auch')[1]
    date_arr = working_dates.split(' ')
    start_date = OebbDate()
    finish_date = OebbDate()
    if 'fährt täglich' in working_dates:
        calendar.monday = True
        calendar.tuesday = True
        calendar.wednesday = True
        calendar.thursday = True
        calendar.friday = True
        calendar.saturday = True
        calendar.sunday = True
        days_done = True

    if 'fährt am' in working_dates:
        index_a = date_arr.index('am')
        try:
            int(date_arr[index_a + 1:][0].replace('.', ''))
        except:
            index_a = date_arr.index('am', index_a + 1)
        index_a = index_a + 1
        start_date = extract_date_from_date_arr(date_arr[index_a:])
        if 'bis' in working_dates:
            index = date_arr.index('bis')
            working_days_from = date_arr[index + 1:]
            finish_date = extract_date_from_date_arr(working_days_from)
        else:
            start_date.year = end_date.year
            finish_date = start_date
        has_fixed_date = True
        merge_date(date1=start_date, date2=finish_date)
    else:
        start_date = begin_date
        finish_date = end_date
        calendar.no_fix_date = True

    date_arr = list(map(lambda x: x.replace(',', ''), date_arr))
    for count, d in enumerate(date_arr):
        if d in official_weekdays:
            days_done = True
            index = count
            if date_arr[index - 1] == '-':
                pass
            elif len(date_arr) > index + 2 and date_arr[index + 1] == '-':
                add_days_to_calendar(calendar, date_arr[index], date_arr[index + 2])
            else:
                add_day_to_calendar(calendar, date_arr[index])
    not_working_calendar_date = []

    if irregular_dates is not None:
        if 'Feiert' in irregular_dates:  # TODO check spelling
            print(irregular_dates, flush=True)
            logger.exception(irregular_dates)
        if 'allg. Feiert' in irregular_dates:  # TODO check spelling
            print(irregular_dates, flush=True)
            logger.exception(irregular_dates)
            calendar.includes_public_holidays = True
            not_working_calendar_date.extend(copy.deepcopy(allg_feiertage))
        extract_date_objects_from_str(not_working_calendar_date, irregular_dates, start_date, finish_date)
    extended_working_dates = []
    if extended_dates is not None:
        extract_date_objects_from_str(extended_working_dates, extended_dates, start_date, finish_date)
    calendar.start_date = int(start_date)
    calendar.end_date = int(finish_date)
    all_working_dates = extended_working_dates
    all_working_dates.extend(not_working_calendar_date)
    all_working_dates.sort(key=lambda x: (int(x), int(x.extend)))
    exceptions_calendar_date: [CalendarDate] = []
    calendar_data_as_string = ''
    exceptions = {}
    for exception_index, exception in enumerate(all_working_dates):
        exception_type = 2 if not exception.extend else 1
        if f'{int(exception)}' in exceptions:
            if exception_type == 1 and exceptions[f'{int(exception)}'].exception_type == 2:
                weekday = datetime.datetime(int(exception.year), int(service_months[exception.month]),
                                            int(exception.day)).weekday()
                if weekday == 0 and not calendar.monday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 1 and not calendar.tuesday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 2 and not calendar.wednesday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 3 and not calendar.thursday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 4 and not calendar.friday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 5 and not calendar.saturday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                elif weekday == 6 and not calendar.sunday:
                    exceptions[f'{int(exception)}'].exception_type = 1
                else:
                    exceptions_calendar_date.remove(exceptions[f'{int(exception)}'])
                continue
            else:
                continue
        exceptions_calendar_date.append(CalendarDate())
        exceptions_calendar_date[-1].exception_type = exception_type
        date = int(exception)
        exceptions_calendar_date[-1].date = date
        exceptions[f'{date}'] = exceptions_calendar_date[-1]
        calendar_data_as_string += f',{date}{exception_type}'
    calendar_data_as_string = calendar_data_as_string[1:]
    if has_fixed_date and not days_done:
        calendar.monday = True
        calendar.tuesday = True
        calendar.wednesday = True
        calendar.thursday = True
        calendar.friday = True
        calendar.saturday = True
        calendar.sunday = True
    return exceptions_calendar_date, calendar_data_as_string, calendar


def extract_dates_from_oebb_page(tree, calendar):
    extra_info_traffic_day = tree.xpath('//*/strong[text() =$first]/../text()', first='Verkehrstage:')
    traffic_day = None

    if extra_info_traffic_day:
        traffic_day = list(filter(lambda x: x.strip() != '', extra_info_traffic_day))
        if traffic_day:
            traffic_day = ' '.join(filter(lambda x: x.strip() != '', traffic_day[0].split('\n'))).replace(';', '')
        else:
            pass
    calendar.monday = False
    calendar.tuesday = False
    calendar.wednesday = False
    calendar.wednesday = False
    calendar.thursday = False
    calendar.friday = False
    calendar.saturday = False
    calendar.sunday = False
    if not extra_info_traffic_day or traffic_day is None or isinstance(traffic_day, list):
        extra_info_traffic_day = tree.xpath('//*/strong[text() =$first]/../*/pre/text()', first='Verkehrstage:')[0]
        extra_info_traffic_day = extra_info_traffic_day.split('\n')[3:]
        extra_info_traffic_day = list(filter(lambda traffic_line: traffic_line.strip() != '', extra_info_traffic_day))
        day_exceptions = []
        today = datetime.date.today()
        current_day = today.day
        current_month = today.month
        current_year = today.year
        first_month = service_months[extra_info_traffic_day[0][0:3]]
        last_month = service_months[extra_info_traffic_day[-1][0:3]]
        short_month = extra_info_traffic_day[0][0:3]
        short_last_month = extra_info_traffic_day[-1][0:3]
        month_as_int = service_months[short_month]
        last_month_as_int = service_months[short_last_month]
        month = extra_info_traffic_day[0].replace(f'{short_month} ', '')
        last_month = extra_info_traffic_day[-1].replace(f'{short_last_month} ', '')
        first_day_in_month = month.find('x') + 1
        last_day_in_month = last_month.rfind('x') + 1
        begin_year = begin_date.year
        if first_month + len(extra_info_traffic_day) - 1 > 12:
            begin_year = begin_date.year
        else:
            first_day_in_month = extra_info_traffic_day[0].find('x') + 1
            if first_day_in_month >= begin_date.day and service_months[
                begin_date.month] <= month_as_int:
                if last_day_in_month <= end_date.day and service_months[
                    end_date.month] >= last_month_as_int and last_month_as_int >= current_month >= month_as_int:
                    begin_year = current_year
                else:
                    begin_year = begin_date.year
            else:
                begin_year = end_date.year

        for index_month, month in enumerate(extra_info_traffic_day):
            short_month = month[0:3]
            month_as_int = service_months[short_month]
            month = month.replace(f'{short_month} ', '')
            year = begin_year
            if month_as_int == 12:
                begin_year += 1
            for day_index, day in enumerate(month):
                if day == 'x':
                    day_exceptions.append((year, month_as_int, day_index + 1))
        day = day_exceptions[0][2]
        month = day_exceptions[0][1]
        year = day_exceptions[0][0]
        if day < 10:
            day = f'0{day}'
        if month < 10:
            month = f'0{month}'
        start_date = int(f'{year}{month}{day}')
        day = day_exceptions[-1][2]
        month = day_exceptions[-1][1]
        year = day_exceptions[-1][0]
        if day < 10:
            day = f'0{day}'
        if month < 10:
            month = f'0{month}'
        finish_date = int(f'{year}{month}{day}')
        calendar.start_date = start_date
        calendar.end_date = finish_date
        exceptions_calendar_date: [CalendarDate] = []
        calendar_data_as_string = ''
        for exception_index, exception in enumerate(day_exceptions):
            exceptions_calendar_date.append(CalendarDate())
            exceptions_calendar_date[-1].exception_type = 1
            day = exception[2]
            month = exception[1]
            year = exception[0]
            if day < 10:
                day = f'0{day}'
            if month < 10:
                month = f'0{month}'
            date = int(f'{year}{month}{day}')
            exceptions_calendar_date[-1].date = date
            calendar_data_as_string += f',{date}{1}'
        calendar_data_as_string = calendar_data_as_string[1:]
        return exceptions_calendar_date, calendar_data_as_string, calendar
    else:
        calendar = extract_date_from_str(calendar, traffic_day)
        return calendar


def save_stop_family(stop_ids, stop_names, main_stop):
    main_stop.location_type = 1
    stop_list = []
    existed = 0
    for ids, name in zip(stop_ids, stop_names):
        p_stop = main_stop.stop_id
        # if ids in existing_stop_ids:
        #    existed += 1
        #    p_stop = None
        child_stop = Stop(stop_name=name, parent_station=p_stop, stop_lat=main_stop.stop_lat,
                          stop_lon=main_stop.stop_lon,
                          stop_url='https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input=' + str(ids),
                          location_type=0, crawled=True, ext_id=ids)
        new_stop = add_stop(child_stop)
        stop_list.append(new_stop)
    if 0 < existed == len(stop_names):
        main_stop.location_type = 0
    return stop_list


def save_stop(stop_id, stop_name):
    new_stop = Stop(stop_name=stop_name,
                    stop_url='https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input=' + str(stop_id),
                    location_type=0, crawled=True, ext_id=stop_id)
    return add_stop(new_stop)


def add_date_to_allg_feiertage(feiertag, year):
    date = OebbDate()
    f_split = feiertag.split(' ')
    date.day = int(f_split[0].replace('.', ''))
    date.month = f_split[1]
    date.year = year
    date.extend = False
    if int(date) < int(begin_date) or int(date) > int(end_date):
        return
    allg_feiertage.append(date)


def load_allg_feiertage():
    try:
        with open(
                pickle_path / f'{begin_date.year}-{begin_date.month}-{begin_date.day}-{end_date.year}-{end_date.month}-{end_date.day}.pickle',
                'rb') as f:
            allg_feiertage.extend(pickle.load(f))
    except:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        SECRET_KEY = os.environ.get('AM_I_IN_A_DOCKER_CONTAINER', False)
        if SECRET_KEY:
            driver = webdriver.Chrome(options=chrome_options)
        else:
            driver = webdriver.Chrome(chrome_driver_path)
        driver.get('https://www.timeanddate.de/feiertage/oesterreich/' + str(begin_date.year))
        WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, '//*/tbody')))
        try:
            cookie_ok = driver.find_element_by_xpath(
                "//*/button[contains(@class, 'fc-button') and contains(@class, 'fc-cta-consent') and contains(@class,'fc-primary-button')]")

            cookie_ok.click()
        except EC.NoSuchElementException:
            pass
        elements = driver.find_elements_by_xpath("//*/tbody/tr[@class='showrow']/th")

        for f in elements:
            add_date_to_allg_feiertage(f.text, begin_date.year)

        driver.get('https://www.timeanddate.de/feiertage/oesterreich/' + str(end_date.year))
        WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, '//*/tbody')))
        elements = driver.find_elements_by_xpath("//*/tbody/tr[@class='showrow']/th")
        for f in elements:
            add_date_to_allg_feiertage(f.text, end_date.year)
        driver.quit()
        with open(
                pickle_path / f'{begin_date.year}-{begin_date.month}-{begin_date.day}-{end_date.year}-{end_date.month}-{end_date.day}.pickle',
                'wb') as f:
            pickle.dump(allg_feiertage, f)


def add_stop_times_from_web_page(tree, page: PageDTO, current_stops_dict, trip):
    headsign = None
    stop_before_current = None
    stop_time_list = []
    for i in range(len(page.stop_names)):
        all_times = list(map(lambda x: x.strip(),
                             tree.xpath('//*/tr[@class=$first or @class=$second][$count]/td[@class=$third]/text()',
                                        first='zebracol-2',
                                        second="zebracol-1", third='center sepline', count=i + 1)))
        if str(all_times[2]).strip() != '' and str(all_times[2]).strip() != page.route.route_long_name.strip():
            headsign = str(all_times[2]).strip()
        stop = current_stops_dict[page.stop_ids[i]]
        new_stop_time = StopTime(stop_id=stop.stop_id, trip_id=trip.trip_id,
                                 arrival_time=all_times[0] if all_times[0] == '' else all_times[0] + ':00',
                                 departure_time=all_times[1] if all_times[1] == '' else all_times[1] + ':00',
                                 stop_sequence=i + 1, pickup_type=0, drop_off_type=0, stop_headsign=headsign)
        if new_stop_time.arrival_time == '':
            new_stop_time.arrival_time = new_stop_time.departure_time
        elif new_stop_time.departure_time == '':
            new_stop_time.departure_time = new_stop_time.arrival_time
        if stop_before_current is not None and int(stop_before_current.departure_time[0:2]) > int(
                new_stop_time.arrival_time[0:2]):
            new_stop_time.arrival_time = f'{int(new_stop_time.arrival_time[0:2]) + 24}{new_stop_time.arrival_time[2:]}'
            new_stop_time.departure_time = f'{int(new_stop_time.departure_time[0:2]) + 24}{new_stop_time.departure_time[2:]}'
        elif int(new_stop_time.arrival_time[0:2]) > int(new_stop_time.departure_time[0:2]):
            new_stop_time.departure_time = f'{int(new_stop_time.departure_time[0:2]) + 24}{new_stop_time.departure_time[2:]}'
        stop_before_current = new_stop_time
        stop_time_list.append(new_stop_time)
    add_stop_times(stop_time_list)


def process_page(url, page: PageDTO):
    if page.calendar_data is None:
        raise CalendarDataNotFoundError(f'no calendar_data')

    if page.agency is None:
        new_agency: Agency = get_from_table_first(Agency)
    else:
        new_agency: Agency = add_agency(page.agency)

    page.route.agency_id = new_agency.agency_id
    route = add_route(page.route)
    calendar = add_calendar_dates(page.calendar_data[0], page.calendar_data[1], page.calendar_data[2])
    new_trip = Trip(route_id=route.route_id, service_id=calendar.service_id, oebb_url=url)
    if new_trip.service_id is None:
        raise Exception(f'no service_id {url}')
    trip: Trip = add_trip(new_trip, f'{page.first_station}{page.dep_time}')
    stops_on_db = get_all_stops_in_list(page.stop_ids)
    current_stops_dict = {i.ext_id: i for i in stops_on_db}
    for i, (stop_name, stop_id) in enumerate(zip(page.stop_names, page.stop_ids)):
        if stop_id not in current_stops_dict:
            stop_url = page.stop_links[i]
            new_stop = Stop(stop_name=stop_name,
                            stop_url=stop_url, location_type=0,
                            ext_id=stop_id)
            new_stop = add_stop_without_check(new_stop)
            current_stops_dict[stop_id] = new_stop
    tree = html.fromstring(page.page)
    stop_times_to_add.append((tree, page, current_stops_dict, trip))


transport_type_not_found = {}


def request_processing_hook(resp, *args, **kwargs):
    route_page = resp
    traffic_day = None
    tree = html.fromstring(route_page.content)
    all_links_of_station = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/@href', first='zebracol-2',
                                      second="zebracol-1")
    crawled_stop_ids_ = [int(parse_qs(urlparse(link).query)['input'][0]) for link in all_links_of_station]

    # If a given station id in the route is already crawled,
    # the route must be already in the database. So we can stop processing
    if any(s_ids in crawled_stop_ids for s_ids in crawled_stop_ids_):
        raise TripAlreadyPresentError

    route_long_name = \
        tree.xpath('((//*/tr[@class=$first])[1]/td[@class=$second])[last()]/text()', first='zebracol-2',
                   second='center sepline')[0].strip()
    all_stations = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/text()', first='zebracol-2',
                              second="zebracol-1")
    extra_info_operator = tree.xpath('//*/strong[text()=$first]/../text()', first='Betreiber:')
    extra_info_remarks = tree.xpath('//*/strong[text()=$first]/../text()', first='Bemerkungen:')
    new_agency = Agency()
    if extra_info_operator:
        operator = list(filter(lambda x: x.strip() != '', extra_info_operator))[0].strip()
        try:
            agency = operator.split(', ')
            if len(agency) == 1:
                agency_phone = None
                agency_name = ','.join(agency[0].split(',')[:-1])
            else:
                agency_name = agency[0]
                agency_phone = agency[1]
            new_agency.agency_lang = 'DE'
            new_agency.agency_timezone = 'Europe/Vienna'
            new_agency.agency_url = 'https://www.google.com/search?q=' + agency_name
            new_agency.agency_name = agency_name
            new_agency.agency_phone = agency_phone
        except:
            pass
    else:
        new_agency = None

    route_info = tree.xpath('//*/span[@class=$first]/img/@src', first='prodIcon')[0]
    route_type = None
    url = resp.url
    try:
        for prefix in oebb_img_prefix:
            if route_info.startswith(prefix):
                route_type = route_types[str(route_info).removeprefix(prefix)]
                break
        else:
            transport_type_not_found[route_info] = url
            route_type = -1
    except KeyError:
        transport_type_not_found[route_info] = url
        route_type = -1
    new_route = Route(route_long_name=route_long_name,
                      route_type=route_type,
                      route_url=url)
    calendar = Calendar()
    try:
        calendar_data = extract_dates_from_oebb_page(tree, calendar)
    except Exception as e:
        logger.exception(e)
        calendar_data = None
    all_times = list(map(lambda x: x.strip(),
                         tree.xpath('//*/tr[@class=$first or @class=$second][$count]/td[@class=$third]/text()',
                                    first='zebracol-2',
                                    second="zebracol-1", third='center sepline', count=1)))
    first_station = all_stations[0]
    dep_time = all_times[1]

    resp.data = PageDTO(new_agency, new_route, calendar_data, first_station, dep_time, all_stations, crawled_stop_ids_,
                        all_links_of_station, resp.content)


def load_data_async(routes):
    future_session = requests_retry_session_async(session=FuturesSession())
    futures = [future_session.get(route, timeout=6, verify=False, hooks={'response': request_processing_hook}) for
               route
               in routes]
    for i in as_completed(futures):
        try:
            response = i.result()
            q.append(response)
        except TripAlreadyPresentError:
            pass
        except (ConnectionError,MaxRetryError,ReadTimeoutError) as error:
            logger.debug(f'Trying again with {error.request.url}')
            futures.append(future_session.get(error.request.url, verify=False, hooks={'response': request_processing_hook}))
        except Exception as e:
            logger.exception(e)
    exit(0)


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in crawled_stop_ids:
        return False

    return stop_is_to_crawl_geometry(stop_to_check)


def load_all_routes_from_stops(stops: List[Stop]):
    future_session_stops = requests_retry_session_async(session=FuturesSession())
    stop_ext_id_dict = {s.ext_id: s for s in stops}

    stop_names = [stop.stop_name for stop in stops]
    stop_ids = [stop.ext_id for stop in stops]
    future_args = []
    for s_n, s_i in zip(stop_names, stop_ids):
        if s_i not in crawled_stop_ids:
            for date in date_arr:
                set_date(date)
                re = 'https://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_scotty.vs_liveticker&evaId=' + str(
                    int(s_i)) + '&boardType=arr&time=00:00' + '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate' + '=period&dateBegin=' + \
                     date_w[0] + '&dateEnd=' + date_w[0] + '&productsFilter=1011111111011'
                future_args.append(re)

    future_routes = [future_session_stops.get(url, timeout=20, verify=False) for url in future_args]

    station_journeys = {}
    for f in as_completed(future_routes):
        try:
            result = f.result()
            routes_of_station = result
            json_data = json.loads(routes_of_station.content.decode('iso-8859-1')[14:-1])
            if json_data is not None and json_data['maxJ'] is not None:
                station_id = int(json_data['stationEvaId'])
                station_journeys[station_id] = list(map(lambda x: x, json_data['journey']))
        except Exception as e:
            logger.exception(e)

    routes = set()
    future_route_url = []

    try:
        for s_id in station_journeys:
            all_transport = get_all_name_of_transport_distinct(station_journeys[s_id])
            stop = stop_ext_id_dict[s_id]
            for route in all_transport:
                try:
                    url = "https://fahrplan.oebb.at/bin/trainsearch.exe/dn"
                    querystring = {"ld": "2"}
                    for date in date_arr:
                        set_date(date)
                        payload = {
                            'trainname': route,
                            'stationname': stop.stop_name,
                            'REQ0JourneyStopsSID': stop.ext_id,
                            'selectDate': 'oneday',
                            'date': f'{weekday_name}, {date_w}',
                            'wDayExt0': 'Mo|Di|Mi|Do|Fr|Sa|So',
                            'periodStart': str(begin_date),
                            'periodEnd': str(end_date),
                            'time': '',
                            'maxResults': 10000,
                            'stationFilter': '81,01,02,03,04,05,06,07,08,09',
                            'start': 'Suchen'
                        }
                        future_response = future_session_stops.post(url, data=payload, params=querystring,
                                                                    verify=False,
                                                                    hooks={'response': response_journey_hook})
                        future_route_url.append(future_response)
                except Exception as e:
                    logger.exception(e)
    except Exception as e:
        logger.exception(e)
    for i in as_completed(future_route_url):
        try:
            result = i.result()
            routes.update(result.data)
        except Exception as e:
            logger.exception(e)
    future_session_stops.close()
    return routes, stop_ext_id_dict.keys()


def crawl():
    global stop_times_to_add, finishUp, date_arr
    try:
        max_stops_to_crawl = getConfig('batchSize')
    except KeyError:
        max_stops_to_crawl = 3
    try:
        date_arr = getConfig('dates')
    except KeyError:
        date_arr = [date_w]

    commit()
    load_allg_feiertage()
    try:
        if getConfig('resetDBstatus'):
            for stop in get_from_table(Stop):
                stop.crawled = False
    except KeyError:
        pass
    commit()
    new_session()
    count12 = 0
    logger.info("starting route crawling")
    while True:
        for stop in (uncrawled := load_all_uncrawled_stops(max_stops_to_crawl, stop_is_to_crawl)):
            stop.crawled = True
        commit()
        if uncrawled is None or len(uncrawled) == 0:
            break
        routes, ext_ids = load_all_routes_from_stops(uncrawled)
        commit()
        stop_times_to_add = []
        t = Thread(target=load_data_async, args=(routes,))
        t.daemon = True
        t.start()
        while t.is_alive() or (not t.is_alive() and len(q) > 0):
            if len(q) == 0:
                time.sleep(0.01)
                continue
            page = q.pop()
            try:
                process_page(page.url, page.data)
            except CalendarDataNotFoundError as e:
                logger.debug(f'Could\'t load {page.url}')
            except TripAlreadyPresentError as e:
                pass
            except Exception as e:
                logger.exception(str(e) + f' with {page.url}')
            except KeyboardInterrupt:
                logger.exception('Keyboard interrupt')
                exit(0)
        stop_times_executor = ThreadPoolExecutor()
        for tree, page, current_stops_dict, trip in stop_times_to_add:
            stop_times_executor.submit(add_stop_times_from_web_page, tree, page, current_stops_dict,
                                       trip)
        stop_times_executor.shutdown(wait=True)
        for t, u in transport_type_not_found.items():
            add_transport_name(t, u)
        transport_type_not_found.clear()
        commit()
        new_session()
        crawled_stop_ids.update(ext_ids)
        count12 = count12 + 1
        logger.info(f'finished route batch {count12 * max_stops_to_crawl}')
    logger.info("finished route crawling")
    commit()
    return count12


def crawl_routes():
    get_std_date()
    crawl()


if __name__ == '__main__':
    crawl_routes()
