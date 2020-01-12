import os

from Models.agency import Agency
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip

from Models.calendar import Calendar

from Models.frequencies import Frequency
from Models.shape import Shape
from Models.transfers import Transfer

try:
    DATABASE_URI = 'postgres+psycopg2://' + str(os.environ['postgres'])
except KeyError:
    DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, aliased

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)

s = Session()


def add_agency(agency):
    data = s.query(Agency).filter(Agency.agency_id == agency.agency_id).first()
    if data is None:
        s.add(agency)
    else:
        agency = data
    return agency


def add_frequency():
    pass


def add_route(route: Route):
    data: Route = s.query(Route).filter(Route.route_long_name == route.route_long_name).first()
    if data is None:
        s.add(route)
    else:
        route = data
    return route


def route_exist(url):
    data: Route = s.query(Route).filter(Route.route_url == url).first()
    return data is not None


def add_shape():
    # shape wont be needed
    pass


def add_stop(stop):
    data: Stop = s.query(Stop).filter(Stop.stop_name == stop.stop_name).first()
    if data is None:
        s.add(stop)
    elif stop.stop_lat is not None or stop.stop_url is not None or stop.location_type is not None:
        if data.stop_lat is None and stop.stop_lat is not None:
            data.stop_lat = stop.stop_lat
            data.stop_lon = stop.stop_lon
        if data.stop_url is None and stop.stop_url is not None:
            data.stop_url = stop.stop_url
        if data.location_type is None and stop.location_type is not None:
            data.location_type = stop.location_type
        commit()
        stop = data
    return stop


def add_stop_time(stoptime: StopTime):
    data: StopTime = s.query(StopTime).filter(
        and_(StopTime.stop_id == stoptime.stop_id, StopTime.trip_id == stoptime.trip_id,
             StopTime.departure_time == stoptime.departure_time, StopTime.arrival_time == stoptime.arrival_time,
             StopTime.stop_sequence == stoptime.stop_sequence)).first()
    if data is None:
        s.add(stoptime)
    else:
        stoptime = data
    return stoptime


def add_transfer():
    pass


def add_calendar(service: Calendar):
    data: Calendar = s.query(Calendar).filter(
        and_(Calendar.start_date == service.start_date, Calendar.end_date == service.end_date,
             Calendar.monday == service.monday, Calendar.tuesday == service.tuesday,
             Calendar.wednesday == service.wednesday, Calendar.thursday == service.thursday,
             Calendar.friday == service.friday, Calendar.saturday == service.saturday,
             Calendar.sunday == service.sunday)).first()
    if data is None:
        s.add(service)
    else:
        service = data
    return service


def add_trip(trip):
    s.add(trip)
    commit()
    s.refresh(trip)
    return trip


def get_from_table(t):
    return s.query(t).all()


def get_from_table_first(t):
    return s.query(t).first()


def get_agencies():
    return s.query(Agency).all()


def get_calendars():
    return s.query(Calendar).all()


def get_frequencies():
    return s.query(Frequency).all()


def get_routes():
    return s.query(Route).all()


def get_shapes():
    return s.query(Shape).all()


def get_stops():
    return s.query(Stop).all()


def get_stop_times():
    return s.query(StopTime).all()


def get_transfers():
    return s.query(Transfer).all()


def get_trips():
    return s.query(Trip).all()


def new_session():
    global s
    try:
        s.close()
    except:
        pass
    s = Session()


def commit():
    s.commit()


def end_session():
    s.close()
