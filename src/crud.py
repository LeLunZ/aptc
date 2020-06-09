import os

from Models.agency import Agency
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip

from Models.calendar import Calendar
from sqlalchemy.dialects.postgresql import aggregate_order_by
from Models.frequencies import Frequency
from Models.shape import Shape
from Models.transfers import Transfer
from Models.calendar_date import CalendarDate
from Models.transport_type_image import TransportTypeImage
from Models.stop_time_text import StopTimeText
from sqlalchemy.exc import SQLAlchemyError

import sqlalchemy

try:
    DATABASE_URI = 'postgres+psycopg2://' + str(os.environ['postgres'])
except KeyError:
    DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine, and_, func, literal_column, Text
from sqlalchemy.sql import functions
from sqlalchemy.orm import sessionmaker, aliased

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)

s = Session(autoflush=False, expire_on_commit=False)


def add_agency(agency):
    data = s.query(Agency).filter(Agency.agency_name == agency.agency_name).first()
    if data is None:
        s.add(agency)
        commit()
        s.refresh(agency)
    else:
        agency = data
    return agency


def add_frequency():
    pass


def add_route(route: Route):
    data: Route = s.query(Route).filter(
        and_(Route.route_long_name == route.route_long_name, Route.agency_id == route.agency_id)).first()
    if data is None:
        s.add(route)
        commit()
        s.refresh(route)
    else:
        route = data
    return route


def route_exist(url):
    data: Trip = s.query(Trip).filter(Trip.oebb_url == url).first()
    return data is not None


def add_shape():
    # shape wont be needed
    pass


def add_transport_name(route, url):
    transport_type_name = TransportTypeImage(name=route, oebb_url=url)
    data = s.query(TransportTypeImage).filter(TransportTypeImage.name == route).first()
    if data is None:
        s.add(transport_type_name)
        commit()
    else:
        pass
    return


def add_stop_time_text(text1):
    stop_time_text = StopTimeText(working_days=text1)
    data = s.query(StopTimeText).filter(StopTimeText.working_days == text1).first()
    if data is None:
        s.add(stop_time_text)
        commit()
    else:
        pass
    return


# deprecated
def add_calendar_dates(calendar_dates: [CalendarDate], only_dates_as_string: str, service: Calendar):
    if only_dates_as_string == '':
        subquery = ~s.query(CalendarDate).filter(CalendarDate.service_id == Calendar.service_id).exists()
        data = s.query(Calendar).filter(subquery).first()
        if data is None:
            s.add(service)
            commit()
            s.refresh(service)
        else:
            service = data
    else:
        aggregate_function = func.string_agg(
            functions.concat(CalendarDate.date.cast(Text), CalendarDate.exception_type.cast(Text)),
            aggregate_order_by(literal_column("','"), CalendarDate.date, CalendarDate.exception_type))
        all_calendar_dates_service_query = s.query(CalendarDate.service_id).group_by(CalendarDate.service_id).having(
            aggregate_function == only_dates_as_string)
        calendar = s.query(Calendar).filter(
            and_(Calendar.service_id.in_(all_calendar_dates_service_query), Calendar.start_date == service.start_date,
                 Calendar.end_date == service.end_date,
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday))
        calendar = calendar.first()
        if calendar is None:
            s.add(service)
            commit()
            s.refresh(service)
            for i in calendar_dates:
                i.service_id = service.service_id
            s.bulk_save_objects(calendar_dates)
            commit()
        else:
            service = calendar
    return service


def add_calendar_dates2(calendar_dates: [CalendarDate], only_dates_as_string: str, service: Calendar):
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
            commit()
            s.refresh(service)
        else:
            service = data
    else:
        hash_value = str(hash(only_dates_as_string))
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
            commit()
            s.refresh(service)
            for i in calendar_dates:
                i.service_id = service.service_id
            s.bulk_save_objects(calendar_dates)
            commit()
        else:
            service = calendar
    return service


def add_stop(stop: Stop):
    data: Stop = s.query(Stop).filter(Stop.stop_name == stop.stop_name).first()
    if data is None:
        s.add(stop)
        commit()
        s.refresh(stop)
    else:
        some_thing_changed = False
        if stop.stop_lat is not None or stop.stop_url is not None or stop.stop_lon is not None:
            if data.stop_lat is None and stop.stop_lat is not None:
                data.stop_lat = stop.stop_lat
                some_thing_changed = True
            if data.stop_lon is None and stop.stop_lon is not None:
                data.stop_lon = stop.stop_lon
                some_thing_changed = True
            if data.stop_url is None and stop.stop_url is not None:
                data.stop_url = stop.stop_url
                some_thing_changed = True
            if some_thing_changed:
                s.merge(data)
                commit()
                s.refresh(data)
        stop = data
    return stop


def add_stop_time(stoptime: StopTime):
    data: StopTime = s.query(StopTime).filter(
        and_(StopTime.stop_id == stoptime.stop_id, StopTime.trip_id == stoptime.trip_id,
             StopTime.departure_time == stoptime.departure_time, StopTime.arrival_time == stoptime.arrival_time,
             StopTime.stop_sequence == stoptime.stop_sequence)).first()
    if data is None:
        s.add(stoptime)
        commit()
        s.refresh(stoptime)
    else:
        stoptime = data
    return stoptime


def add_transfer():
    pass


def add_calendar(service: Calendar):
    if service.service_id is not None:
        data: Calendar = s.query(Calendar).filter(
            and_(Calendar.service_id == service.service_id, Calendar.start_date == service.start_date,
                 Calendar.end_date == service.end_date,
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday)).first()
    else:
        data: Calendar = s.query(Calendar).filter(
            and_(Calendar.start_date == service.start_date, Calendar.end_date == service.end_date,
                 Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
                 Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
                 Calendar.friday == service.friday, Calendar.saturday == service.saturday,
                 Calendar.sunday == service.sunday)).first()
    if data is None:
        service.service_id = None
        s.add(service)
        commit()
        s.refresh(service)
    else:
        service = data
    return service


def add_trip(trip, hash1):
    data: Trip = s.query(Trip).filter(and_(Trip.service_id == trip.service_id, Trip.route_id == trip.route_id,
                                           trip.station_departure_time_hash == hash1)).first()
    trip.station_departure_time_hash = hash1
    if data is None:
        s.add(trip)
        commit()
        s.refresh(trip)
    else:
        raise Exception('Trip already exists! noob.')
    return trip


def get_from_table(t):
    return s.query(t).all()


def get_from_table_first(t):
    return s.query(t).first()

def new_session():
    global s
    end_session()
    s = Session(autoflush=False)


def query_element(e):
    return s.query(e)


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

    Requires a database that supports window functions,
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
