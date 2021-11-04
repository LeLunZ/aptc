import logging
import threading
from functools import wraps

import sqlalchemy
import xxhash

from Classes.exceptions import TripAlreadyPresentError
from Functions.config import getConfig
from Functions.timing import timing
from Models.agency import Agency
from Models.calendar import Calendar
from Models.calendar_date import CalendarDate
from Models.route import Route
from Models.stop import Stop
from Models.stop_time_text import StopTimeText
from Models.stop_times import StopTime
from Models.transport_type_image import TransportTypeImage
from Models.trip import Trip

logger = logging.getLogger(__name__)
lock = threading.Lock()
try:
    DATABASE_URI = 'postgresql+psycopg2://' + str(getConfig('postgres'))
except KeyError:
    DATABASE_URI = 'postgresql+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URI, executemany_mode='values')

Session = sessionmaker(bind=engine, autoflush=True, autocommit=False, expire_on_commit=True)

s = Session()


def lockF(lock):
    def wrap(func):
        @wraps(func)
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
        agency.set_id()
        s.add(agency)
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
        route.set_id()
        s.add(route)
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
    else:
        pass
    return


def add_stop_time_text(text1):
    stop_time_text = StopTimeText(working_days=text1)
    data = s.query(StopTimeText).filter(StopTimeText.working_days == text1).first()
    if data is None:
        s.add(stop_time_text)
    else:
        pass
    return


@timing
def add_calendar_dates(calendar_dates: [CalendarDate], only_dates_as_string: str, service: Calendar):
    if only_dates_as_string == '':
        data = s.query(Calendar).filter(
            and_(Calendar.calendar_dates_hash == None, or_(and_(Calendar.start_date == service.start_date,
                                                                Calendar.end_date == service.end_date),
                                                           and_(Calendar.no_fix_date == True,
                                                                service.no_fix_date == True)),
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday)).first()
        service.calendar_dates_hash = None
        if data is None:
            service.set_id()
            s.add(service)
        else:
            if data.no_fix_date and service.no_fix_date:
                data.start_date = service.start_date
                data.end_date = service.end_date
            service = data
    else:
        hash_value = xxhash.xxh3_64(only_dates_as_string).intdigest()
        calendar = s.query(Calendar).filter(
            and_(Calendar.calendar_dates_hash == hash_value, or_(and_(Calendar.start_date == service.start_date,
                                                                      Calendar.end_date == service.end_date),
                                                                 and_(Calendar.no_fix_date == True,
                                                                      service.no_fix_date == True)),
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday))
        calendar = calendar.first()
        if calendar is None:
            service.calendar_dates_hash = hash_value
            service.set_id()
            s.add(service)
            for i in calendar_dates:
                i.service_id = service.service_id
            s.add_all(calendar_dates)
        else:
            if calendar.no_fix_date and service.no_fix_date:
                calendar.start_date = service.start_date
                calendar.end_date = service.end_date
            service = calendar
    return service


def get_all_stops_in_list(stops):
    data = s.query(Stop).filter(Stop.ext_id.in_(stops)).all()
    return data


def get_all_ext_id_from_crawled_stops():
    data = s.query(Stop.ext_id).filter(and_(Stop.crawled == True, Stop.is_allowed == True)).all()
    return data


def get_all_ext_id_from_stops():
    data = s.query(Stop.ext_id).all()
    return data


def get_all_ext_id_from_stops_where_siblings_searched():
    data = s.query(Stop.ext_id).filter(Stop.siblings_searched == True).all()
    return data


def get_all_ext_id_from_stops_where_info_searched():
    data = s.query(Stop.ext_id).filter(Stop.info_searched == True).all()
    return data


def get_all_names_from_searched_stops():
    data = s.query(Stop.stop_name).filter(Stop.siblings_searched == False).all()
    return data


def add_stop_without_check(stop: Stop):
    stop.set_id()
    s.add(stop)
    return stop


def add_stop(stop: Stop):
    data: Stop = s.query(Stop).filter(Stop.ext_id == stop.ext_id).first()
    if data is None:
        stop.set_id()
        s.add(stop)
    else:
        if stop.crawled is True and (data.crawled is False or data.crawled is None):
            data.crawled = True
        if stop.parent_station is not None and data.parent_station is None:
            data.parent_station = stop.parent_station
        if data.location_type == 0 and stop.location_type != data.location_type:
            data.location_type = stop.location_type
        if data.prod_class is None and stop.prod_class is not None:
            data.prod_class = stop.prod_class
        if data.ext_id is None and stop.ext_id is not None:
            data.ext_id = stop.ext_id
        stop = data
    return stop


def get_stops_with_parent():
    return s.query(Stop).filter(
        and_(Stop.parent_station != None, or_(Stop.stop_lon == None, Stop.stop_lat == None))).all()


def get_stop_with_id(parent_id):
    return s.query(Stop).filter(Stop.stop_id == parent_id).first()


@lockF(lock)
def remove_parent_from_all():
    try:
        s.query(Stop).filter(Stop.parent_station != None).update({Stop.parent_station: None})
    except Exception as e:
        logger.exception(e)


def load_all_uncrawled_stops(max_stops_to_crawl, check_stop_method):
    all_stops = []
    while len(all_stops) == 0:
        stops = s.query(Stop).filter(
            and_(Stop.crawled == False, Stop.info_searched == True, Stop.prod_class != None)).order_by(Stop.ext_id,
                                                                                                       Stop.prod_class).limit(
            max_stops_to_crawl).all()
        if len(stops) == 0:
            break

        all_stops = []
        for stop in stops:
            if check_stop_method(stop):
                all_stops.append(stop)
                all_stops.extend(
                    s.query(Stop).filter(and_(Stop.crawled == False, or_(Stop.group_ext_id.like(f'%,{stop.ext_id}'),
                                                                         Stop.group_ext_id.like(f'{stop.ext_id},%'),
                                                                         Stop.group_ext_id.like(f'%,{stop.ext_id},%'),
                                                                         Stop.group_ext_id.like(
                                                                             f'{stop.ext_id}')))).all())
            else:
                stop.crawled = True
                stop.is_allowed = False

    return set(all_stops)

    # return s.query(Stop).filter(and_(Stop.crawled == False, Stop.prod_class != None)).order_by(Stop.ext_id,
    #                                                                                           Stop.prod_class).limit(
    #    max_stops_to_crawl).all()


def sibling_search_stops(max_stops_to_crawl):
    return s.query(Stop).filter(or_(Stop.siblings_searched == False, Stop.info_searched == False)).order_by(Stop.ext_id,
                                                                                                            Stop.prod_class).limit(
        max_stops_to_crawl).all()


def get_all_stops_without_location(max_stops_to_crawl):
    return s.query(Stop).filter(and_(Stop.stop_lat == None, Stop.prod_class == None)).order_by(Stop.ext_id,
                                                                                               Stop.prod_class).limit(
        max_stops_to_crawl).all()


@lockF(lock)
def update_location_of_stop(stop, lat, lng):
    try:
        s.query(Stop).filter(stop.stop_id == Stop.stop_id).update({Stop.stop_lat: lat, Stop.stop_lon: lng})
    except Exception as e:
        logger.exception(e)


def add_stop_time(stoptime: StopTime):
    s.add(stoptime)
    return stoptime


def add_stop_times(stoptime):
    s.add_all(stoptime)


def add_transfer():
    pass


def add_trip(trip, hash1):
    trip.station_departure_time = hash1
    data: Trip = s.query(Trip).filter(and_(Trip.service_id == trip.service_id, Trip.route_id == trip.route_id,
                                           Trip.station_departure_time == trip.station_departure_time)).first()
    if data is None:
        trip.set_id()
        s.add(trip)
    else:
        raise TripAlreadyPresentError(f'Trip with id {data.trip_id} already present.')
    return trip


def get_from_table(t):
    return s.query(t).all()


def get_from_table_with_filter(t, f):
    return s.query(t).filter(f).all()


def get_stops_without_location():
    return s.query(Stop).filter(Stop.stop_lon == None).all()


def get_from_table_first(t):
    return s.query(t).first()


@lockF(lock)
def new_session():
    global s
    end_session()
    s = Session()


def query_element(e):
    return s.query(e)


def rollback():
    global s
    try:
        s.rollback()
    except:
        new_session()


@lockF(lock)
def commit():
    global s
    try:
        s.commit()
    except Exception as e:
        logger.exception(e)
        s.rollback()

        def new_session_nested():
            global s
            end_session()
            s = Session()

        new_session_nested()


def end_session():
    try:
        s.close()
    except Exception as e:
        logger.exception(e)


def get_last_agency_id():
    return s.query(Agency.agency_id).filter(func.max(Agency.agency_id)).first()


def get_last_calendar_id():
    return s.query(Calendar.service_id).filter(func.max(Calendar.service_id)).first()


def get_last_route_id():
    return s.query(Route.route_id).filter(func.max(Route.route_id)).first()


def get_last_stop_id():
    return s.query(Stop.stop_id).filter(func.max(Stop.stop_id)).first()


def get_last_trip_id():
    return s.query(Trip.trip_id).filter(func.max(Trip.trip_id)).first()


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
