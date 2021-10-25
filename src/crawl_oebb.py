import copy
import datetime
import json
import pickle
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from pathlib import Path
from threading import Thread
from typing import List
from urllib.parse import parse_qs, unquote, urlparse

from lxml import html
from requests_futures.sessions import FuturesSession
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
# import fiona
# from shapely.geometry import shape
from selenium.webdriver.support.wait import WebDriverWait

from Classes.DTOs import PageDTO
from Classes.oebb_date import OebbDate, service_months, begin_date, end_date, get_std_date
from Functions.helper import add_day_to_calendar, extract_date_from_date_arr, merge_date, add_days_to_calendar, \
    get_all_name_of_transport_distinct, extract_date_objects_from_str
from Functions.oebb_requests import requests_retry_session_async, date_w, set_date, weekday_name
from Functions.request_hooks import request_station_id_processing_hook, response_journey_hook
from crud import *

finishUp = False

import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(filename='./Data/aptc.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

try:
    logging.getLogger("requests").setLevel(logging.CRITICAL)
except:
    pass

try:
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
except:
    pass

try:
    logging.getLogger("selenium").setLevel(logging.CRITICAL)
except:
    pass

q = []
route_types = {
    '/img/vs_oebb/rex_pic.gif': 2,
    '/img/vs_oebb/r_pic.gif': 2,
    '/img/vs_oebb/s_pic.gif': 1,
    '/img/vs_oebb/os_pic.gif': 2,
    '/img/vs_oebb/ex_pic.gif': 2,
    '/img/vs_oebb/hog_pic.gif': 3,
    '/img/vs_oebb/nmg_pic.gif': 3,
    '/img/vs_oebb/ntr_pic.gif': 0,
    '/img/vs_oebb/hmp_pic.gif': 3,
    '/img/vs_oebb/bus_pic.gif': 3,
    '/img/vs_oebb/str_pic.gif': 0,
    '/img/vs_oebb/er_pic.gif': 2,
    '/img/vs_oebb/en_pic.gif': 2,
    '/img/vs_oebb/atb_pic.gif': 115,
    '/img/vs_oebb/arz_pic.gif': 115,
    '/img/vs_oebb/ice_pic.gif': 101,
    '/img/vs_oebb/d_pic.gif': 2,
    '/img/vs_oebb/ast_pic.gif': 1500,
    '/img/vs_oebb/rb_pic.gif': 2,
    '/img/vs_oebb/bsv_pic.gif': 3,
    '/img/vs_oebb/cat_pic.gif': 1,
    '/img/vs_oebb/rj_pic.gif': 2,
    '/img/vs_oebb/nj_pic.gif': 2,
    '/img/vs_oebb/ec_pic.gif': 102,
    '/img/vs_oebb/ic_pic.gif': 102,
    '/img/vs_oebb/obu_pic.gif': 11,
    '/img/vs_oebb/icb_pic.gif': 3,
    '/img/vs_oebb/dps_pic.gif': 2,
    '/img/vs_oebb/u_pic.gif': 1,
    '/img/vs_oebb/wb_pic.gif': 2,
    '/img/vs_oebb/rjx_pic.gif': 2,
    '/img/vs_oebb/cjx_pic.gif': 2,
    '/img/vs_oebb/t84_pic.gif': 1700,
    '/img/vs_oebb/x70_pic.gif': 1700,
    '/img/vs_oebb/u70_pic.gif': 1700,
    '/img/vs_oebb/sb_pic.gif': 6,
    '/img/vs_oebb/lkb_pic.gif': 0,
    '/img/vs_oebb/rer_pic.gif': 109,
    '/img/vs_oebb/sch_pic.gif': 1000,
    '/img/vs_oebb/rgj_pic.gif': 2,
    '/img/vs_oebb/val_pic.gif': 3,
    '/img/vs_oebb/rfb_pic.gif': 715,
    '/img/vs_oebb/lil_pic.gif': 107,
    '/img/vs_oebb/bmz_pic.gif': 107,
    '/img/vs_oebb/ter_pic.gif': 106,
    '/img/vs_oebb/dpn_pic.gif': 106
}
date_arr = []
stop_times_executor = None
stop_times_to_add = []
crawled_stop_ids = set([e.ext_id for e in get_all_ext_id_from_crawled_stops()])
existing_stop_ids = set([e.ext_id for e in get_ext_id_from_crawled_stops_where_main()])
allg_feiertage = []
crawlStopOptions = None
fiona_geometry = None
southLatBorder = None
northLatBorder = None
westLonBorder = None
eastLonBorder = None


def extract_date_from_str(calendar: Calendar, date_str: str, add=True):
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
        if 'allg. Feiertg' in irregular_dates:
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
    if not add:
        return exceptions_calendar_date, calendar_data_as_string, calendar
    calendar = add_calendar_dates(exceptions_calendar_date, calendar_data_as_string, calendar)
    return calendar


def extract_dates_from_oebb_page(tree, calendar, add=True):
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
        if add:
            calendar = add_calendar_dates(exceptions_calendar_date, calendar_data_as_string, calendar)
        else:
            return (exceptions_calendar_date, calendar_data_as_string, calendar)
    else:
        calendar = extract_date_from_str(calendar, traffic_day, add)
        if not add:
            return calendar
    return calendar


def save_stop_family(stop_ids, stop_names, main_stop):
    main_stop.location_type = 1
    stop_list = []
    existed = 0
    for ids, name in zip(stop_ids, stop_names):
        p_stop = main_stop.stop_id
        if ids in existing_stop_ids:
            existed += 1
            p_stop = None
        child_stop = Stop(stop_name=name, parent_station=p_stop, stop_lat=main_stop.stop_lat,
                          stop_lon=main_stop.stop_lon,
                          stop_url='https://fahrplan.oebb.at/bin/stboard.exe/dn?protocol=https:&input=' + str(ids),
                          location_type=0, crawled=True, ext_id=ids)
        new_stop = add_stop(child_stop)
        stop_list.append(new_stop)
    if 0 < existed == len(stop_names):
        main_stop.location_type = 0
    return stop_list


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
                f'Data/{begin_date.year}-{begin_date.month}-{begin_date.day}-{end_date.year}-{end_date.month}-{end_date.day}.pickle',
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
            driver = webdriver.Chrome('./chromedriver')
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
                f'Data/{begin_date.year}-{begin_date.month}-{begin_date.day}-{end_date.year}-{end_date.month}-{end_date.day}.pickle',
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
        raise Exception(f'no calendar_data')

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


def request_processing_hook(resp, *args, **kwargs):
    route_page = resp
    traffic_day = None
    tree = html.fromstring(route_page.content)
    all_links_of_station = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/@href', first='zebracol-2',
                                      second="zebracol-1")
    crawled_stop_ids = [int(parse_qs(urlparse(link).query)['input'][0]) for link in all_links_of_station]

    # If a given station id in the route is already crawled,
    # the route must be already in the database. So we can stop processing
    if any(s_ids in crawled_stop_ids for s_ids in crawled_stop_ids):
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
        if route_info.startswith('/img'):
            route_type = route_types[route_info]
        else:
            p = Path(route_info)
            route_type = route_types[str('/' / Path(*p.parts[2:]))]
    except KeyError:
        add_transport_name(route_info, url)
        route_type = -1
    new_route = Route(route_long_name=route_long_name,
                      route_type=route_type,
                      route_url=url)
    calendar = Calendar()
    try:
        calendar_data = extract_dates_from_oebb_page(tree, calendar, False)
    except:
        calendar_data = None
    all_times = list(map(lambda x: x.strip(),
                         tree.xpath('//*/tr[@class=$first or @class=$second][$count]/td[@class=$third]/text()',
                                    first='zebracol-2',
                                    second="zebracol-1", third='center sepline', count=1)))
    first_station = all_stations[0]
    dep_time = all_times[1]

    resp.data = PageDTO(new_agency, new_route, calendar_data, first_station, dep_time, all_stations, crawled_stop_ids,
                        all_links_of_station, resp.content)


def load_data_async(routes):
    future_session = requests_retry_session_async(session=FuturesSession())
    futures = [future_session.get(route, timeout=3, verify=False, hooks={'response': request_processing_hook}) for
               route
               in routes]
    for i in as_completed(futures):
        try:
            response = i.result()
            q.append(response)
        except TripAlreadyPresentError:
            pass
        except Exception as e:
            print(f'{i} {str(e)}', flush=True)
    exit(0)


def stop_is_to_crawl(stop_to_check: Stop) -> bool:
    if stop_to_check.ext_id in crawled_stop_ids:
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


def load_all_routes_from_stops(stops: List[Stop]):
    future_session_stops = requests_retry_session_async(session=FuturesSession())
    futures_stops_args = []
    stop_ext_id_dict = {}

    for stop in stops:
        querystring = {"ld": "3"}
        payload = {
            'sqView': '1&input=' + stop.input + '&time=21:42&maxJourneys=50&dateBegin=&dateEnd=&selectDate=&productsFilter=0000111011&editStation=yes&dirInput=&',
            'input': stop.input,
            'inputRef': stop.input + '#' + str(stop.ext_id),
            'sqView=1&start': 'Information aufrufen',
            'productsFilter': '0000111011'
        }
        if stop_is_to_crawl(stop):
            stop_ext_id_dict[stop.ext_id] = stop
            futures_stops_args.append(("https://fahrplan.oebb.at/bin/stboard.exe/dn", payload, querystring))

    futures_stops = [future_session_stops.post(url, data=payload, params=querystring, verify=False,
                                               hooks={'response': request_station_id_processing_hook}) for
                     (url, payload, querystring) in futures_stops_args]
    current_station_ids = set()
    future_args = []
    if len(futures_stops) == 0:
        return []
    for f in as_completed(futures_stops):
        try:
            result = f.result()
            all_stations = result.data  # get all station ids from response
            station_names = list(
                map(lambda x: unquote(''.join(x.split('|')[:-1]), encoding='iso-8859-1'), all_stations))
            station_ids = list(map(lambda x: int(x.split('|')[-1]), all_stations))
            new_stop = False
            for s_n, s_i in zip(station_names, station_ids):
                if s_i not in crawled_stop_ids and s_i not in current_station_ids:
                    new_stop = True
                    current_station_ids.add(s_i)
                    for date in date_arr:
                        set_date(date)
                        re = 'https://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_scotty.vs_liveticker&evaId=' + str(
                            int(s_i)) + '&boardType=arr&time=00:00' + '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate' + '=period&dateBegin=' + \
                             date_w[0] + '&dateEnd=' + date_w[0] + '&productsFilter=1011111111011'
                        future_args.append(re)
            if new_stop:
                if len(result.data) > 1:
                    stops = save_stop_family(station_ids[1:], station_names[1:], stop_ext_id_dict[station_ids[0]])
                    for stop in stops:
                        stop_ext_id_dict[stop.ext_id] = stop
                else:
                    stop = stop_ext_id_dict[station_ids[0]]
                    stop.crawled = True
            else:
                logging.debug(f'not new stops {station_names}')
        except Exception as e:
            logging.error("Skipped one station: " + str(e))

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
            print(str(e), flush=True)

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
                    logging.error(
                        f'get_all_routes_of_transport_and_station {route} {stop.name} {str(e)}')
    except:
        pass
    for i in as_completed(future_route_url):
        try:
            result = i.result()
            routes.update(result.data)
        except:
            pass
    future_session_stops.close()
    return routes, stop_ext_id_dict.keys()


def crawl():
    global stop_times_to_add, finishUp, date_arr
    try:
        max_stops_to_crawl = getConfig('batchSize')
    except:
        max_stops_to_crawl = 3
    try:
        date_arr = getConfig('dates')
    except:
        date_arr = [date_w]

    commit()
    load_allg_feiertage()
    try:
        if getConfig('resetDBstatus'):
            for stop in get_from_table(Stop):
                stop.crawled = False
    except:
        pass
    commit()
    new_session()
    count12 = 0
    while True:
        for stop in (uncrawled := load_all_uncrawled_stops(max_stops_to_crawl)):
            stop.crawled = True
        commit()
        if uncrawled is None or len(uncrawled) == 0:
            break
        routes, ext_ids = load_all_routes_from_stops(uncrawled)
        stop_times_to_add = []
        t = Thread(target=load_data_async, args=(routes,))
        t.daemon = True
        t.start()
        commit()
        while t.is_alive() or (not t.is_alive() and len(q) > 0):
            if len(q) == 0:
                time.sleep(0.01)
                continue
            page = q.pop()
            try:
                process_page(page.url, page.data)
            except TripAlreadyPresentError as e:
                pass
            except Exception as e:
                logging.error(f'load_route {page.url} {repr(e)}')
            except KeyboardInterrupt:
                exit(0)
        stop_times_executor = ThreadPoolExecutor()
        for tree, page, current_stops_dict, trip in stop_times_to_add:
            stop_times_executor.submit(add_stop_times_from_web_page, tree, page, current_stops_dict,
                                       trip)
        stop_times_executor.shutdown(wait=True)
        commit()
        new_session()
        crawled_stop_ids.update(ext_ids)
        count12 = count12 + 1
        logging.debug(f'finished batch {count12 * max_stops_to_crawl}')
    logging.debug("finished crawling")
    logging.debug("adding all other stops")
    logging.debug("stops finished now")
    commit()
    return count12


def crawl_routes():
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
        except KeyError:
            pass
        except FileNotFoundError:
            pass
        except Exception as e:
            pass

        if crawlStopOptions:
            northLatBorder = getConfig('crawlStopOptions.northLatBorder')
            southLatBorder = getConfig('crawlStopOptions.southLatBorder')
            westLonBorder = getConfig('crawlStopOptions.westLonBorder')
            eastLonBorder = getConfig('crawlStopOptions.eastLonBorder')
    except KeyError as e:
        crawlStopOptions = False
    get_std_date()
    crawl()


if __name__ == '__main__':
    crawl_routes()
