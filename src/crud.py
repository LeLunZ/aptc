import os
import threading
from typing import List

from Functions.config import getConfig
from Models.agency import Agency
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip
from Models.calendar import Calendar
from Models.calendar_date import CalendarDate
from Models.transport_type_image import TransportTypeImage
from Models.stop_time_text import StopTimeText
import sqlalchemy
import pyhash

hasher = pyhash.fnv1a_64()
lock = threading.Lock()
try:
    DATABASE_URI = 'postgres+psycopg2://' + str(getConfig('postgres'))
except KeyError:
    DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine, and_, or_, func, literal_column, Text
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URI, executemany_mode='values')

Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=True)

s = Session()


class TripAlreadyPresentError(Exception):
    pass


def lockF(lock):
    def wrap(func):
        def wrapped(*args):
            lock.acquire()
            try:
                ret = func(*args)
            except Exception as e:
                raise e
            finally:
                lock.release()
            return ret

        return wrapped

    return wrap


def add_agency(agency):
    data = s.query(Agency).filter(Agency.agency_name == agency.agency_name).first()
    if data is None:
        s.add(agency)
        s.flush()
    else:
        agency = data
    return agency


def add_frequency():
    pass


def add_route(route: Route):
    data: Route = s.query(Route).filter(
        and_(Route.route_type == route.route_type, Route.route_long_name == route.route_long_name,
             Route.agency_id == route.agency_id)).first()
    if data is None:
        s.add(route)
        s.flush()
    else:
        route = data
    return route


def add_shape():
    # shape wont be needed
    pass


def add_transport_name(route, url):
    transport_type_name = TransportTypeImage(name=route, oebb_url=url)
    data = s.query(TransportTypeImage).filter(TransportTypeImage.name == route).first()
    if data is None:
        s.add(transport_type_name)
        s.flush()
    else:
        pass
    return


def add_stop_time_text(text1):
    stop_time_text = StopTimeText(working_days=text1)
    data = s.query(StopTimeText).filter(StopTimeText.working_days == text1).first()
    if data is None:
        s.add(stop_time_text)
        s.flush()
    else:
        pass
    return


def add_calendar_dates(calendar_dates: [CalendarDate], only_dates_as_string: str, service: Calendar):
    if only_dates_as_string == '':
        data = s.query(Calendar).filter(
            and_(Calendar.calendar_dates_hash == None, Calendar.start_date == service.start_date,
                 Calendar.end_date == service.end_date,
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday)).first()
        if data is None:
            s.add(service)
            s.flush()
        else:
            service = data
    else:
        hash_value = hasher(only_dates_as_string)
        calendar = s.query(Calendar).filter(
            and_(Calendar.calendar_dates_hash == hash_value, Calendar.start_date == service.start_date,
                 Calendar.end_date == service.end_date,
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday))
        calendar = calendar.first()
        if calendar is None:
            service.calendar_dates_hash = hash_value
            s.add(service)
            s.flush()
            for i in calendar_dates:
                i.service_id = service.service_id
            s.bulk_save_objects(calendar_dates)
            s.flush()
        else:
            service = calendar
    return service


def get_all_stops_in_list(stops):
    data = s.query(Stop).filter(Stop.stop_name.in_(stops)).all()
    return data


def add_stop_without_check(stop: Stop):
    s.add(stop)
    s.flush()
    return stop


def add_stop(stop: Stop):
    data: Stop = s.query(Stop).filter(Stop.stop_name == stop.stop_name).first()
    if data is None:
        s.add(stop)
        s.flush()
    else:
        if stop.crawled is True and (data.crawled is False or data.crawled is None):
            data.crawled = True
            s.flush()
        stop = data
    return stop


def load_all_uncrawled_stops(max_stops_to_crawl):
    return s.query(Stop).filter(or_(Stop.crawled == None, Stop.crawled == False)).order_by(Stop.stop_lat,
                                                                                           Stop.stop_lon).limit(
        max_stops_to_crawl).all()


@lockF(lock)
def update_location_of_stop(stop, lat, lng):
    try:
        s.query(Stop).filter(stop.stop_id == Stop.stop_id).update({Stop.stop_lat: lat, Stop.stop_lon: lng})
    except:
        print("fucked up. Object probably not in session", flush=True)


def add_stop_time(stoptime: StopTime):
    s.add(stoptime)
    return stoptime


def add_stop_times(stoptime):
    s.bulk_save_objects(stoptime)


def add_transfer():
    pass


def add_trip(trip, hash1):
    trip.station_departure_time = hash1
    data: Trip = s.query(Trip).filter(and_(Trip.service_id == trip.service_id, Trip.route_id == trip.route_id,
                                           Trip.station_departure_time == trip.station_departure_time)).first()
    if data is None:
        s.add(trip)
        s.flush()
    else:
        raise TripAlreadyPresentError()
    return trip


def get_from_table(t):
    return s.query(t).all()


def get_stops_without_location():
    return s.query(Stop).filter(or_(Stop.stop_lon == None, Stop.stop_lat == None)).all()


def get_from_table_first(t):
    return s.query(t).first()


@lockF(lock)
def new_session():
    global s
    end_session()
    s = Session()


def query_element(e):
    return s.query(e)


@lockF(lock)
def commit():
    s.commit()


def end_session():
    try:
        s.close()
    except:
        pass


def column_windows(session, column, windowsize):
    """Return a series of WHERE clauses against
    a given column that break it into windows.

    Result is an iterable of tuples, consisting of
    ((start, end), whereclause), where (start, end) are the ids.

    Requires a database that supports window Functions,
    i.e. Postgresql, SQL Server, Oracle.

    Enhance this yourself !  Add a "where" argument
    so that windows of just a subset of rows can
    be computed.

    """

    def int_for_range(start_id, end_id):
        if end_id:
            return and_(
                column >= start_id,
                column < end_id
            )
        else:
            return column >= start_id

    q = session.query(
        column,
        func.row_number(). \
            over(order_by=column). \
            label('rownum')
    ). \
        from_self(column)
    if windowsize > 1:
        q = q.filter(sqlalchemy.text("rownum %% %d=1" % windowsize))

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)


def windowed_query(q, column, windowsize):
    """"Break a Query into windows on a given column."""

    for whereclause in column_windows(
            q.session,
            column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row
