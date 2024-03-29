import copy
import json
import logging
import pickle
import time
from queue import Queue
from threading import Thread

import fiona
from shapely.geometry import shape, mapping, Point, Polygon, MultiPolygon
import googlemaps
from requests import Response
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
from Functions.config import getConfig
from Functions.oebb_requests import requests_retry_session, requests_retry_session_async, date_w, set_date, weekday_name
from urllib.parse import parse_qs, unquote

finishUp = False

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


def save_simple_stops(names, ids, main_station):
    if len(ids) > 1:
        for index, (name, id) in enumerate(zip(names, ids)):
            if index == 0:
                main_station.crawled = True
                main_station = add_stop(main_station)
                continue
            new_stop = Stop(stop_name=name, parent_station=main_station.stop_id, stop_lat=main_station.stop_lat,
                            stop_lon=main_station.stop_lon,
                            location_type=0, crawled=True)
            new_stop = add_stop(new_stop)
    else:
        main_station.crawled = True
        main_station = add_stop(main_station)


def export_all_tables():
    tables = [Agency, Calendar, CalendarDate, Frequency, Route, Trip, StopTime, Shape, Stop, Transfer]
    file_names = []
    os.chdir('./db')
    try:
        os.remove('./Archiv.zip')
    except FileNotFoundError:
        pass
    try:
        excluded_routes = getConfig('exportOptions.excludeRouteTypes')
    except:
        excluded_routes = None
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

    if excluded_routes is not None:
        print("removing routes")
        all_routes = []
        all_trips = []
        all_stop_times = []
        deleted_route_ids = []
        deleted_trip_ids = []
        with open(f'./{Route.__table__.name}.txt', 'r') as routes:
            first_line_routes = routes.readline()
            route_type_index = Route.firstline().index('route_type')
            route_id_index = Route.firstline().index('route_id')
            csv_reader = csv.reader(routes, delimiter=',')
            for route in csv_reader:
                if route[route_type_index] != '' and int(route[route_type_index]) in excluded_routes:
                    deleted_route_ids.append(int(route[route_id_index]))
                    continue
                all_routes.append(route)

        with open(f'./{Trip.__table__.name}.txt', 'r') as trips:
            first_line_trips = trips.readline()
            route_id_of_trip_index = Trip.firstline().index('route_id')
            trip_id_index = Trip.firstline().index('trip_id')
            csv_reader = csv.reader(trips, delimiter=',')
            for trip in csv_reader:
                if int(trip[route_id_of_trip_index]) in deleted_route_ids:
                    deleted_trip_ids.append(int(trip[trip_id_index]))
                    continue
                all_trips.append(trip)

        with open(f'./{StopTime.__table__.name}.txt', 'r') as stop_times:
            first_line_stop_times = stop_times.readline()
            trip_id_of_stop_time_index = StopTime.firstline().index("trip_id")
            csv_reader = csv.reader(stop_times, delimiter=',')
            for stop_time in csv_reader:
                if int(stop_time[trip_id_of_stop_time_index]) in deleted_trip_ids:
                    continue
                all_stop_times.append(stop_time)

        os.remove(f'./{Route.__table__.name}.txt')

        with open(f'./{Route.__table__.name}.txt', 'a') as routes:
            routes.writelines([first_line_routes])
            outcsv = csv.writer(routes, delimiter=',')
            for row in all_routes:
                outcsv.writerow(row)

        os.remove(f'./{Trip.__table__.name}.txt')

        with open(f'./{Trip.__table__.name}.txt', 'a') as trips:
            trips.writelines([first_line_trips])
            outcsv = csv.writer(trips, delimiter=',')
            for row in all_trips:
                outcsv.writerow(row)

        os.remove(f'./{StopTime.__table__.name}.txt')

        with open(f'./{StopTime.__table__.name}.txt', 'a') as stop_times:
            stop_times.writelines([first_line_stop_times])
            outcsv = csv.writer(stop_times, delimiter=',')
            for row in all_stop_times:
                outcsv.writerow(row)
        print(f"done removing routes with type {excluded_routes}")

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
    global stop_dict, finishUp
    session = requests_retry_session_async(session=FuturesSession(max_workers=1))
    while True:
        try:
            stop = real_thread_safe_q.get(block=False)
            if stop.stop_name.split('(')[0].strip() in stop_dict:
                try:
                    coords = stop_dict[stop.stop_name.split('(')[0].strip()]
                    update_location_of_stop(stop, coords['y'], coords['x'])
                except:
                    pass
                finally:
                    real_thread_safe_q.task_done()
            else:
                try:
                    future_1 = session.get(
                        'http://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' +
                        stop.stop_name.split('(')[0].strip() + '?&js=false&',
                        verify=False)
                    oebb_location = future_1.result()
                    locations = oebb_location.content[8:-22]
                    stop_suggestions = json.loads(locations.decode('iso-8859-1'))
                    for stop_suggestion in stop_suggestions['suggestions']:
                        stop_dict[stop_suggestion['value']] = {'y': str_to_geocord(stop_suggestion['ycoord']),
                                                               'x': str_to_geocord(stop_suggestion['xcoord'])}
                    if stop.stop_name.split('(')[0].strip() in stop_dict:
                        current_station_cord = stop_dict.pop(stop.stop_name.split('(')[0].strip())
                        update_location_of_stop(stop, current_station_cord['y'], current_station_cord['x'])
                    else:
                        pass  # dont do anything
                except:
                    pass
                finally:
                    real_thread_safe_q.task_done()
        except:
            if finishUp:
                print("finished stop thread")
                exit(0)
            print("sleeping 30 seconds")
            time.sleep(30)


def match_station_with_parents():
    all_stops = get_stops_with_parent()
    parent_id_stop_dict = {}
    for stop in all_stops:
        if stop.parent_station not in parent_id_stop_dict:
            parent_id_stop_dict[stop.parent_station] = [stop]
        else:
            parent_id_stop_dict[stop.parent_station].append(stop)
    for id in parent_id_stop_dict:
        parent_stop = get_stop_with_id(id)
        childs = parent_id_stop_dict[id]
        for child in childs:
            update_location_of_stop(child, parent_stop.stop_lat, parent_stop.stop_lon)
    remove_parent_from_all()
    commit()


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
    tree = html.fromstring(page.page)
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
    futures = [future_session.get(route, timeout=3, verify=False, hooks={'response': request_processing_hook}) for
               route
               in routes]
    count = 0
    for i in as_completed(futures):
        try:
            count += 1
            response = i.result()
            q.append(response)
        except Exception as e:
            print(f'{i} {str(e)}', flush=True)
    exit(0)


def add_all_empty_to_queue():
    all_stops = get_stops_without_location()
    for stop in all_stops:
        stop_dto = StopDTO(stop.stop_name, stop.stop_url, stop.location_type, stop.stop_id)
        if stop_dto not in list(real_thread_safe_q.queue) and (stop.stop_lat is None or stop.stop_lon is None):
            real_thread_safe_q.put(stop_dto)


def request_stops_processing_hook(resp, *args, **kwargs):
    locations = resp.content[8:-22]
    locations = json.loads(locations.decode('iso-8859-1'))
    suggestion = locations['suggestions']
    main_station = suggestion[0]
    stops = [main_station]
    for se in suggestion:
        stops.append(Stop(stop_name=se['value'],
                          stop_lat=str_to_geocord(se['ycoord']),
                          stop_lon=str_to_geocord(se['xcoord']), location_type=0))
    resp.data = stops


def request_station_id_processing_hook(resp, *args, **kwargs):
    tree = html.fromstring(resp.content)
    all_stations = tree.xpath('//*/option/@value')
    resp.data = all_stations


station_ids = set()


def load_all_stops_to_crawl(stop_names):
    future_session_stops = requests_retry_session_async(session=FuturesSession())
    futures = [future_session_stops.get(
        'http://fahrplan.oebb.at/bin/ajax-getstop.exe/dn?REQ0JourneyStopsS0A=1&REQ0JourneyStopsB=12&S=' +
        stop_name.split('(')[0].strip() + '?&js=false&',
        verify=False, timeout=6, hooks={'response': request_stops_processing_hook}) for
        stop_name
        in stop_names]
    futures_stops = []
    futures_stops_args = []
    stop_name_dict = {}
    count = 0
    for i in as_completed(futures):
        try:
            response = i.result()
            r = response.data
            querystring = {"ld": "3"}
            payload = {
                'sqView': '1&input=' + r[0][  # Linz
                    'value'] + '&time=21:42&maxJourneys=50&dateBegin=&dateEnd=&selectDate=&productsFilter=0000111011&editStation=yes&dirInput=&',
                'input': r[0]['value'],
                'inputRef': r[0]['value'] + '#' + str(int(r[0]['extId'])),
                'sqView=1&start': 'Information aufrufen',
                'productsFilter': '0000111011'
            }
            stop_to_crawl: Stop = r[1]  # take second stop because first one is already crawled Wien
            fiona_geometry_is_avaible = fiona_geometry is not None and fiona_geometry is not False and (
                        type(fiona_geometry) is list and len(fiona_geometry) > 0)
            point = shape({'type': 'Point', 'coordinates': [stop_to_crawl.stop_lon, stop_to_crawl.stop_lat]})
            point_is_within = False
            if fiona_geometry_is_avaible:
                for k in fiona_geometry:
                    if point.within(k):
                        point_is_within = True
            if (fiona_geometry_is_avaible and point_is_within) or (
                    not fiona_geometry_is_avaible and not crawlStopOptions) or (
                    crawlStopOptions and southLatBorder < stop_to_crawl.stop_lat < northLatBorder and westLonBorder < stop_to_crawl.stop_lon < eastLonBorder):
                stop_name_dict[r[1].stop_name] = (r[0], r[1])
                futures_stops_args.append(("https://fahrplan.oebb.at/bin/stboard.exe/dn", payload, querystring))
        except Exception as e:
            pass

    station_id_list = []
    futures_stops = [future_session_stops.post(url, data=payload, params=querystring, verify=False,
                                               hooks={'response': request_station_id_processing_hook}) for
                     (url, payload, querystring) in futures_stops_args]
    route_url_dict = {}
    future_args = []
    if len(futures_stops) == 0:
        return []
    for f in as_completed(futures_stops):
        try:
            result = f.result()
            all_station_ids = result.data
            all_station_names = None
            data = parse_qs(result.request.body)
            inputRef = data['inputRef'][0]
            original_name = inputRef.split('#')[0]
            if all_station_ids is None or not all_station_ids:
                original_id = inputRef.split('#')[1]
                all_station_ids = [original_id]
            else:
                all_station_names = list(
                    map(lambda x: unquote(''.join(x.split('|')[:-1]), encoding='iso-8859-1'), all_station_ids))
                all_station_ids = list(map(lambda x: x.split('|')[-1], all_station_ids))
            current_station_ids = []
            for si in all_station_ids:
                if si not in station_ids:
                    station_ids.add(si)
                    current_station_ids.append(si)
                    for date in date_arr:
                        set_date(date)
                        re = 'http://fahrplan.oebb.at/bin/stboard.exe/dn?L=vs_scotty.vs_liveticker&evaId=' + str(
                            int(
                                si)) + '&boardType=arr&time=00:00' + '&additionalTime=0&maxJourneys=100000&outputMode=tickerDataOnly&start=yes&selectDate' + '=period&dateBegin=' + \
                             date_w[0] + '&dateEnd=' + date_w[0] + '&productsFilter=1011111111011'
                        route_url_dict[re] = stop_name_dict[original_name][0]
                        future_args.append(re)
            if len(current_station_ids) > 0:
                station_id_list.append((stop_name_dict[original_name][0], current_station_ids))
                save_simple_stops(all_station_names, all_station_ids, stop_name_dict[original_name][1])
        except Exception as e:
            logging.error("Skipped one station")

    future_routes = [future_session_stops.get(url, timeout=20, verify=False) for url in future_args]
    main_station_journey_dict = {}
    for f in as_completed(future_routes):
        try:
            result = f.result()
            routes_of_station = result
            main_station = route_url_dict[result.url]
            if main_station['id'] not in main_station_journey_dict:
                main_station_journey_dict[main_station['id']] = [main_station]
            json_data = json.loads(routes_of_station.content.decode('iso-8859-1')[14:-1])
            if json_data is not None and json_data['maxJ'] is not None:
                main_station_journey_dict[main_station['id']].extend(
                    list(map(lambda x: x, json_data['journey'])))
        except Exception as e:
            print(str(e), flush=True)

    routes = set()
    future_route_url = []

    def response_journey_hook(resp, *args, **kwargs):
        tree = html.fromstring(resp.content)
        all_routes = tree.xpath('//*/td[@class=$name]/a/@href', name='fcell')
        all_routes = list(map(lambda l: l.split('?')[0], all_routes))
        resp.data = all_routes

    try:
        for i in main_station_journey_dict:
            all_transport = get_all_name_of_transport_distinct(main_station_journey_dict[i][1:])
            main_station = main_station_journey_dict[i][0]
            for route in all_transport:
                try:
                    url = "http://fahrplan.oebb.at/bin/trainsearch.exe/dn"
                    querystring = {"ld": "2"}
                    for date in date_arr:
                        set_date(date)
                        payload = {
                            'trainname': route,
                            'stationname': main_station['value'],
                            'REQ0JourneyStopsSID': main_station['id'],
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
                        future_response = future_session_stops.post(url, data=payload, params=querystring, verify=False,
                                                                    hooks={'response': response_journey_hook})
                        future_route_url.append(future_response)
                except Exception as e:
                    logging.error(
                        f'get_all_routes_of_transport_and_station {route} {main_station} {str(e)}')
    except:
        pass
    for i in as_completed(future_route_url):
        try:
            result = i.result()
            routes.update(result.data)
        except:
            pass
    future_session_stops.close()
    return routes


date_arr = []


def crawl():
    global stop_times_to_add, finishUp, update_stops_thread, date_arr
    with open('Data/bus_stops.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        row_count = sum(1 for row in csv_reader)
        try:
            begin = int(getConfig('csv.begin')) - 1
        except KeyError:
            begin = 1
        try:
            end = int(getConfig('csv.end')) - 1
        except KeyError:
            end = row_count - 1
        csv_file.seek(0)
        stop_set = set()
        for row in skip_stop(csv_reader, begin, end):
            stop_set.add(row[0])
    try:
        max_stops_to_crawl = getConfig('batchSize')
    except:
        max_stops_to_crawl = 3
    try:
        date_arr = getConfig('dates')
    except:
        date_arr = [date_w]
    stop_list = list(stop_set)
    stop_list_deleted = False
    commit()
    get_std_date()
    load_allg_feiertage()
    update_stops_thread = Thread(target=location_data_thread)
    update_stops_thread.daemon = True
    update_stops_thread.start()
    try:
        if getConfig('resetDBstatus'):
            for stop in get_from_table(Stop):
                stop.crawled = False
    except:
        pass
    commit()
    new_session()
    print("started crawling", flush=True)
    count12 = 0
    while True:
        if stop_list_deleted or len(stop_list) == 0:
            if not stop_list_deleted:
                logging.debug(f'deleting stop_list starting with database crawl')
                del stop_list
                stop_list_deleted = True
            if not continuesCrawling:
                break
            stop_set = set()
            for stop in (uncrawled := load_all_uncrawled_stops(max_stops_to_crawl)):
                stop_set.add(stop.stop_name)
                stop.crawled = True
            commit()
            if uncrawled is None or len(uncrawled) == 0:
                break
            to_crawl = list(stop_set)
        else:
            to_crawl = stop_list[:max_stops_to_crawl]
            stop_list = stop_list[max_stops_to_crawl:]
        routes = load_all_stops_to_crawl(to_crawl)
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
        stop_times_executor = ThreadPoolExecutor()
        for tree, page, current_stops_dict, trip in stop_times_to_add:
            stop_times_executor.submit(add_stop_times_from_web_page, tree, page, current_stops_dict,
                                       trip)
        stop_times_executor.shutdown(wait=True)
        commit()
        new_session()
        count12 = count12 + 1
        logging.debug(f'finished batch {count12 * max_stops_to_crawl}')
    logging.debug("finished crawling")
    logging.debug("adding all other stops")
    add_all_empty_to_queue()
    finishUp = True
    logging.debug("waiting to finish now")
    while len(real_thread_safe_q.queue) > 0:
        print(f'sleeping ten seconds')
        time.sleep(10)
        print(f'{len(real_thread_safe_q.queue)} more stops to crawl')
    logging.debug("stops finished now")
    commit()


def match_station_with_google_maps():
    key = getConfig('googleMapsKey')
    gmaps = googlemaps.Client(key=key)
    all_stops = get_stops_without_location()
    for stop in all_stops:
        geocoding = gmaps.geocode(stop.stop_name)
        if geocoding is None or len(geocoding) == 0:
            print(f'couldnt find {stop.stop_name} on google maps')
        else:
            location = geocoding[0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            stop.stop_lat = lat
            stop.stop_lon = lng
    commit()


if __name__ == "__main__":
    try:
        continuesCrawling = getConfig('continues')
    except KeyError as e:
        continuesCrawling = False
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
        except:
            pass

        if crawlStopOptions:
            northLatBorder = getConfig('crawlStopOptions.northLatBorder')
            southLatBorder = getConfig('crawlStopOptions.southLatBorder')
            westLonBorder = getConfig('crawlStopOptions.westLonBorder')
            eastLonBorder = getConfig('crawlStopOptions.eastLonBorder')
    except KeyError as e:
        crawlStopOptions = False
    try:
        if url := getConfig('url'):
            update_stops_thread = Thread(target=location_data_thread)
            update_stops_thread.daemon = True
            update_stops_thread.start()
            process_page(url, None)
            real_thread_safe_q.join()
            commit()
    except KeyError:
        pass
    try:
        if getConfig('crawl'):
            crawl()
    except KeyError as e:
        pass
    try:
        if getConfig('matchWithParent'):
            match_station_with_parents()
    except KeyError as e:
        pass
    try:
        if getConfig('useGoogleMaps'):
            match_station_with_google_maps()
    except KeyError:
        pass
    try:
        if getConfig('export'):
            print('exporting all tables', flush=True)
            export_all_tables()
    except KeyError:
        pass
exit(0)
