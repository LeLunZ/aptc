import csv
import datetime
import logging
import os
from functools import wraps
from time import time
from zipfile import ZipFile

import googlemaps
from Models.calendar import Calendar
from Classes.oebb_date import begin_date, end_date, OebbDate, service_months

from Scripts.crud import commit, get_stops_with_parent, get_stop_with_id, \
    update_location_of_stop, \
    remove_parent_from_all, new_session, query_element, windowed_query, get_from_table, end_session
from Functions.config import getConfig

from Models.agency import Agency
from Models.calendar_date import CalendarDate
from Models.frequencies import Frequency
from Models.route import Route
from Models.shape import Shape
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.transfers import Transfer
from Models.trip import Trip
from constants import db_path

inv_map = {v: k for k, v in service_months.items()}

logger = logging.getLogger(__name__)

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logger.debug(f'Timing - func:{f.__name__} args:[{args}, {kw}] took: i%2.4f sec' % (te-ts))
        return result
    return wrap

def get_all_name_of_transport_distinct(list_of_transport):
    all_transport = set()
    for transport in list_of_transport:
        if transport['pr'] not in all_transport:
            all_transport.add(transport['pr'])
    return list(all_transport)


def remove_param_from_url(url, to_remove):
    splitted_url = url.split(to_remove)
    left_part = splitted_url[0]
    some_python = splitted_url[1].split('&')[::-1][:-1]
    right_part = '&'.join(list(filter(lambda x: x.strip() != '', some_python)))
    return left_part + '&' + right_part


def extract_date_from_date_arr(date_arr: [str]):
    date = OebbDate()
    date.day = int(date_arr[0].replace('.', ''))
    date.month = None
    date.year = None
    if len(date_arr) > 1 and date_arr[1] in service_months:
        date.month = date_arr[1]
        try:
            date.year = int(date_arr[2])
        except:
            pass
    return date


def merge_date(date1: OebbDate, date2: OebbDate):
    if date1.month is None:
        date1.month = date2.month
    if date1.year is None and date2.year is None:
        date1.year = end_date.year
        date2.year = end_date.year
    if date1.year is None and date2.year is not None:
        date1.year = date2.year


def add_day_to_calendar(calendar: Calendar, week_day: str):
    if week_day == 'Mo':
        calendar.monday = True
    if week_day == 'Di':
        calendar.tuesday = True
    if week_day == 'Mi':
        calendar.wednesday = True
    if week_day == 'Do':
        calendar.thursday = True
    if week_day == 'Fr':
        calendar.friday = True
    if week_day == 'Sa':
        calendar.saturday = True
    if week_day == 'So':
        calendar.sunday = True


def add_days_to_calendar(calendar: Calendar, begin: str, end: str):
    official_weekdays = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    for d in official_weekdays[official_weekdays.index(begin):official_weekdays.index(end) + 1]:
        add_day_to_calendar(calendar, d)


def str_to_geocord(cord: str):
    return int(cord) / 1000000


def skip_stop(seq, begin, end):
    for i, item in enumerate(seq):
        if begin <= i <= end and item[-1] and item[-1] != '':
            yield item


def extract_date_objects_from_str(dates_list, dates, start_date, finish_date):
    date_arr = dates.replace('.', '').replace(',', '').split(' ')
    while 'bis' in date_arr:
        index = date_arr.index('bis')
        possible_1 = date_arr[index - 1].replace('.', '')
        first_index = None
        last_index = None
        date = None
        date2 = None
        try:
            day = int(possible_1) if 0 < int(possible_1) < 32 else None
            year = int(possible_1) if day is None else None
            if year is None:
                date = extract_date_from_date_arr(date_arr[index - 1:index])
                first_index = index - 1
            else:
                date = extract_date_from_date_arr(date_arr[index - 3:index])
                first_index = index - 3
        except:
            month = possible_1
            if month in service_months:
                date = extract_date_from_date_arr(date_arr[index - 2:index])
                first_index = index - 2
        if date is not None:
            possible_1 = date_arr[index + 1].replace('.', '')
            try:
                day = int(possible_1) if 0 < int(possible_1) < 32 else None  # leave it here. for exception
                date2 = extract_date_from_date_arr(date_arr[index + 1:index + 4])
                last_index = index + 4
            except:
                date2 = None
        if date is not None and date2 is not None:
            merge_date(date, date2)
            newDate = None
            while not (date.day == date2.day and date.month == date2.month and date.year == date2.year):
                try:
                    newDate = OebbDate()
                    newDate.day = date.day
                    newDate.month = date.month
                    newDate.year = date.year
                    newDate.extend = False
                    dates_list.append(newDate)
                    datetime.datetime(date.year, service_months[date.month], date.day + 1)
                    date.day = date.day + 1
                except:
                    try:
                        datetime.datetime(date.year, service_months[date.month] + 1, 1)
                        date.month = inv_map[service_months[date.month] + 1]
                        date.day = 1
                    except:
                        try:
                            datetime.datetime(date.year + 1, 1, 1)
                            date.year = date.year + 1
                            date.month = 'Jan'
                            date.day = 1
                        except:
                            pass
            if newDate is not None:
                newDate = OebbDate()
                newDate.day = date.day
                newDate.month = date.month
                newDate.year = date.year
                newDate.extend = False
                dates_list.append(newDate)
        for i in range(first_index, last_index):
            date_arr.pop(first_index)
    date_arr.reverse()
    year = None
    month = None
    for count, d in enumerate(date_arr):
        try:
            test_day = int(d)
            if test_day > 31:
                raise Exception()
            if year is not None and month is not None:
                try:
                    day = int(d)
                    date = OebbDate()
                    date.year = year
                    date.month = month
                    date.day = day
                    date.extend = False
                    dates_list.append(date)
                    continue
                except:
                    pass
            if year is None and month is not None:
                try:
                    day = int(d)
                    date = OebbDate()
                    if service_months[month] > service_months[finish_date.month] or (
                            service_months[month] is service_months[finish_date.month] and day > finish_date.day):
                        date.year = start_date.year
                    else:
                        date.year = finish_date.year
                    date.month = month
                    date.day = day
                    date.extend = False
                    dates_list.append(date)
                    continue
                except:
                    pass
        except:
            pass
        try:
            year = int(d)
            if year < finish_date.year or year > finish_date.year:
                raise Exception()
            continue
        except:
            year = None
        if d in service_months:
            month = d
            continue
        else:
            month = None


def match_station_with_google_maps(stops):
    key = getConfig('googleMapsKey')
    gmaps = googlemaps.Client(key=key)
    all_stops = stops
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


def match_station_with_parents():
    all_stops = get_stops_with_parent()
    parent_id_stop_dict = {}
    for stop in all_stops:
        if stop.stop_lat is None or stop.stop_lon is None:
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


def export_all_tables():
    tables = [Agency, Calendar, CalendarDate, Frequency, Route, Trip, StopTime, Shape, Stop, Transfer]
    file_names = []
    os.chdir(db_path)
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

    if excluded_routes is not None:
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

    with ZipFile(f'./GTFS_{str(begin_date)}-{str(end_date)}.zip', 'w') as zip:
        for file in file_names:
            zip.write(file)
