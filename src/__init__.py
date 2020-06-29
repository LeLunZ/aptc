import copy
import json
import logging
import pickle
import time
from queue import Queue
from threading import Thread

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed, ThreadPoolExecutor

from Classes.DTOs import StopDTO, PageDTO
from Functions.helper import add_day_to_calendar, extract_date_from_date_arr, merge_date, add_days_to_calendar, \
    str_to_geocord, \
    remove_param_from_url, skip_stop, get_all_name_of_transport_distinct, extract_date_objects_from_str
from Classes.oebb_date import OebbDate, service_months, begin_date, end_date, get_std_date
from Functions.oebb_requests import requests_retry_session, requests_retry_session_async, get_all_routes_from_station, \
    get_location_suggestion_from_string, get_all_routes_of_transport_and_station, get_all_station_ids_from_station

if __name__ == "__main__":
    from Models.agency import Agency
    from Models.route import Route
    from Models.stop import Stop
    from Models.stop_times import StopTime
    from Models.trip import Trip

    from Models.calendar import Calendar

    from Models.frequencies import Frequency
    from Models.shape import Shape
    from Models.transfers import Transfer
    from Models.calendar_date import CalendarDate
    from crud import *

from selenium import webdriver
import csv
from lxml import html

import os
from zipfile import ZipFile
import datetime

stop_dict = {}

allg_feiertage = []

try:
    logging.getLogger("requests").setLevel(logging.FATAL)
except:
    pass

try:
    logging.getLogger("urllib3").setLevel(logging.FATAL)
except:
    pass

logging.basicConfig(filename='./Data/aptc.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

q = []
real_thread_safe_q = Queue()

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

days_name = {
    0: 'Mo',
    1: 'Di',
    2: 'Mi',
    3: 'Do',
    4: 'Fr',
    5: 'Fr',
    6: 'Fr',
}

stop_times_executor = None


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
    calendar.thursday = False
    calendar.friday = False
    calendar.saturday = False
    calendar.sunday = False
    if not extra_info_traffic_day or traffic_day is None or isinstance(traffic_day, list):
        extra_info_traffic_day = tree.xpath('//*/strong[text() =$first]/../*/pre/text()', first='Verkehrstage:')[0]
        extra_info_traffic_day = extra_info_traffic_day.split('\n')[3:]
        extra_info_traffic_day = list(filter(lambda traffic_line: traffic_line.strip() != '', extra_info_traffic_day))
        months_with_first_and_last_day = []
        day_exceptions = []
        for index_month, month in enumerate(extra_info_traffic_day):
            short_month = month[0:3]
            month_as_int = service_months[short_month]
            month = month.replace(f'{short_month} ', '')
            last_day_in_month = month.rfind('x') + 1
            first_day_in_month = month.find('x') + 1
            if index_month == 0 and month_as_int == 12 and len(extra_info_traffic_day) > 1:
                year = begin_date.year
            else:
                year = end_date.year
            for day_index, day in enumerate(month):
                if day == 'x':
                    day_exceptions.append((year, month_as_int, day_index + 1))
            months_with_first_and_last_day.append((month_as_int, first_day_in_month, last_day_in_month))
        day = months_with_first_and_last_day[0][1]
        month = months_with_first_and_last_day[0][0]
        year = end_date.year
        if day < 10:
            day = f'0{day}'
        if month < 10:
            month = f'0{month}'
        if month == 12 and len(months_with_first_and_last_day) > 1:
            year = begin_date.year
        start_date = int(f'{year}{month}{day}')
        day = months_with_first_and_last_day[-1][2]
        month = months_with_first_and_last_day[-1][0]
        year = end_date.year
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


def save_simple_stops(names, ids, main_station):
    if len(ids) > 1:
        for index, (name, id) in enumerate(zip(names, ids)):
            if index == 0:
                main_station = add_stop(main_station)
                continue
            new_stop = Stop(stop_name=name, stop_lat=main_station.stop_lat, stop_lon=main_station.stop_lon,
                            location_type=0)
            new_stop = add_stop(new_stop)
    else:
        main_station = add_stop(main_station)


def export_all_tables():
    tables = [Agency, Calendar, CalendarDate, Frequency, Route, Shape, Stop, StopTime, Transfer, Trip]
    file_names = []
    os.chdir('./db')
    try:
        os.remove('./Archiv.zip')
    except FileNotFoundError:
        pass
    for i in tables:
        try:
            os.remove(f'./{i.__table__.name}.txt')
        except FileNotFoundError:
            pass
    for i in tables:
        file_names.append(f'./{i.__table__.name}.txt')
        new_session()
        if i is StopTime:
            q = query_element(i)
            with open(f'./{i.__table__.name}.txt', 'a') as outfile:
                outcsv = csv.writer(outfile, delimiter=',')
                outcsv.writerow(i.firstline())
                for dataset in windowed_query(q, StopTime.stop_times_id, 1000):
                    outcsv.writerow(dataset.tocsv())
        else:
            with open(f'./{i.__table__.name}.txt', 'a') as outfile:
                outcsv = csv.writer(outfile, delimiter=',')
                outcsv.writerow(i.firstline())
                records = get_from_table(i)
                for row in records:
                    outcsv.writerow(row.tocsv())
        end_session()
        print(f'finished {i.__table__.name}', flush=True)

    with ZipFile('./Archiv.zip', 'w') as zip:
        for file in file_names:
            zip.write(file)


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


def location_data_thread():
    global stop_dict
    already_done = set()
    session = requests_retry_session_async(session=FuturesSession(max_workers=1))
    while True:
        stop = real_thread_safe_q.get()
        if stop.stop_name in stop_dict:
            coords = stop_dict.pop(stop.stop_name)
            try:
                update_location_of_stop(stop, coords['y'], coords['x'])
            except:
                pass
        else:
            try:
                future_1 = session.get(
                    'http://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' + stop.stop_name + '?&js=false&',
                    verify=False)
                oebb_location = future_1.result()
                locations = oebb_location.content[8:-22]
                stop_suggestions = json.loads(locations.decode('iso-8859-1'))
                for index, stop_suggestion in enumerate(stop_suggestions['suggestions']):
                    if stop_suggestion['value'] not in already_done or index == 0:
                        stop_dict[stop_suggestion['value']] = {'y': str_to_geocord(stop_suggestion['ycoord']),
                                                               'x': str_to_geocord(stop_suggestion['xcoord'])}
                        already_done.add(stop_suggestion['value'])
                current_station_cord = stop_dict.pop(stop_suggestions['suggestions'][0]['value'])
                update_location_of_stop(stop, current_station_cord['y'], current_station_cord['x'])
                real_thread_safe_q.task_done()
            except:
                real_thread_safe_q.task_done()
                real_thread_safe_q.put(stop)


def add_stop_times_from_web_page(tree, page, current_stops_dict, trip):
    headsign = None
    stop_before_current = None
    stop_time_list = []
    for i in range(len(page.all_stations)):
        all_times = list(map(lambda x: x.strip(),
                             tree.xpath('//*/tr[@class=$first or @class=$second][$count]/td[@class=$third]/text()',
                                        first='zebracol-2',
                                        second="zebracol-1", third='center sepline', count=i + 1)))
        if str(all_times[2]).strip() != '' and str(all_times[2]).strip() != page.route.route_long_name.strip():
            headsign = str(all_times[2]).strip()
        stop = current_stops_dict[page.all_stations[i]]
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


stop_times_to_add = []


def process_page(url, page: PageDTO):
    if page is None:
        response = requests_retry_session().get(url, timeout=10, verify=False)
        page = request_processing_hook(response, None, None)  # TODO check if it is working
    if page.calendar_data is None:
        raise Exception(f'no calendar_data')
    tree = html.fromstring(page.page)
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
    stops_on_db = get_all_stops_in_list(page.all_stations)
    current_stops_dict = {i.stop_name: i for i in stops_on_db}
    for i, stop in enumerate(page.all_stations):
        if stop not in current_stops_dict:
            new_stop = Stop(stop_name=stop,
                            stop_url=remove_param_from_url(page.links_of_all_stations[i], '&time='), location_type=0)
            new_stop = add_stop_without_check(new_stop)
            stop_dto = StopDTO(new_stop.stop_name, new_stop.stop_url, new_stop.location_type, new_stop.stop_id)
            # if stop_dto not in real_thread_safe_q.queue and (new_stop.stop_lat is None or new_stop.stop_lon is None): TODO Check if dont needed
            real_thread_safe_q.put(stop_dto)
            current_stops_dict[stop] = new_stop
    stop_times_to_add.append((tree, page, current_stops_dict, trip))


def request_processing_hook(resp, *args, **kwargs):
    route_page = resp
    traffic_day = None
    tree = html.fromstring(route_page.content)
    route_long_name = \
        tree.xpath('((//*/tr[@class=$first])[1]/td[@class=$second])[last()]/text()', first='zebracol-2',
                   second='center sepline')[0].strip()
    all_stations = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/text()', first='zebracol-2',
                              second="zebracol-1")
    all_links_of_station = tree.xpath('//*/tr[@class=$first or @class=$second]/*/a/@href', first='zebracol-2',
                                      second="zebracol-1")
    extra_info_operator = tree.xpath('//*/strong[text() =$first]/../text()', first='Betreiber:')
    extra_info_remarks = tree.xpath('//*/strong[text() =$first]/../text()', first='Bemerkungen:')
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
        route_type = route_types[route_info]
    except KeyError:
        add_transport_name(route_info, url)
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
    resp.data = PageDTO(new_agency, new_route, calendar_data, first_station, dep_time, all_stations,
                        all_links_of_station, resp.content)


def load_data_async(routes):
    future_session = requests_retry_session_async(session=FuturesSession())
    futures = [future_session.get(route, timeout=300, verify=False, hooks={'response': request_processing_hook}) for
               route
               in routes]
    for i in as_completed(futures):
        try:
            response = i.result()
            q.append(response)
        except Exception as e:
            print(f'{str(e)} connection timeout', flush=True)
    exit(0)


def add_all_empty_to_queue():
    all_stops = get_stops_without_location()
    for stop in all_stops:
        stop_dto = StopDTO(stop.stop_name, stop.stop_url, stop.location_type, stop.stop_id)
        if stop_dto not in real_thread_safe_q.queue and (stop.stop_lat is None or stop.stop_lon is None):
            real_thread_safe_q.put(stop_dto)


if __name__ == "__main__":
    try:
        if os.environ['export']:
            export_all_tables()
            exit()
    except KeyError:
        pass
    try:
        url = os.environ['url']
        if url:
            update_stops_thread = Thread(target=location_data_thread)
            update_stops_thread.daemon = True
            update_stops_thread.start()
            process_page(url, None)
            real_thread_safe_q.join()
            commit()
            exit(0)
    except KeyError:
        pass

    with open('Data/bus_stops.csv') as csv_file:
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
        for count, i in enumerate(csv_reader):
            if count != 0 and i[7] != '' and i[8] != '':
                stop_dict[i[0]] = {'x': float(i[7]), 'y': float(i[8])}
        csv_file.seek(0)
        csv_reader = csv.reader(csv_file, delimiter=',')
        get_std_date()
        load_allg_feiertage()
        commit()
        update_stops_thread = Thread(target=location_data_thread)
        update_stops_thread.daemon = True
        update_stops_thread.start()
        print("started crawling", flush=True)
        for row in skip_stop(csv_reader, begin, end):
            stop_times_to_add = []
            time_now = time.time()
            new_session()
            try:
                location_data = get_location_suggestion_from_string(row[0])
                suggestion = location_data['suggestions']
                main_station = suggestion[0]
                try:
                    new_stop = Stop(stop_name=main_station['value'],
                                    stop_lat=str_to_geocord(main_station['ycoord']),
                                    stop_lon=str_to_geocord(main_station['xcoord']), location_type=0)
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
                        while (json_data is None or json_data['maxJ'] is None) and count < 2:
                            json_data = get_all_routes_from_station(station_id)
                            count += 1
                        if json_data is not None and json_data['maxJ'] is not None:
                            public_transportation_journey.extend(
                                list(map(lambda x: x, json_data['journey'])))
                    routes = set()
                    try:
                        all_transport = get_all_name_of_transport_distinct(public_transportation_journey)
                        for route in all_transport:
                            try:
                                routes.update([yx.split('?')[0] for yx in
                                               get_all_routes_of_transport_and_station(route, main_station)])
                            except Exception as e:
                                logging.error(
                                    f'get_all_routes_of_transport_and_station {route} {main_station} {str(e)}')
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
                        stop_times_executor = ThreadPoolExecutor()
                        for tree, page, current_stops_dict, trip in stop_times_to_add:
                            stop_times_executor.submit(add_stop_times_from_web_page, tree, page, current_stops_dict,
                                                       trip)
                        stop_times_executor.shutdown(wait=True)
                        print("finished batch", flush=True)
                    except Exception as e:
                        log_error(f'get_all_name_of_transport_distinct {public_transportation_journey} {str(e)}')
                except Exception as e:
                    logging.error(f"get_all_station_ids_from_station {main_station} {str(e)}")
            except Exception as e:
                logging.error(f'get_location_suggestion_from_string {row} {str(e)}')
            logging.debug(f"finished {row}")
            commit()
            print(f'{(time.time() - time_now) / 60} min.', flush=True)
        add_all_empty_to_queue()
        real_thread_safe_q.join()
        commit()
exit(0)
