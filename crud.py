from Models.agency import *
from Models.route import Route
from Models.stop import Stop
from Models.stop_times import StopTime
from Models.trip import Trip

DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine
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


def add_calendar():
    pass


def add_frequency():
    pass


def add_route(route):
    data: Route = s.query(Route).filter(
        Route.route_id == route.route_id).first()
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


def add_stop_time(stoptime):
    data: StopTime = s.query(StopTime).filter(StopTime.trip_id == stoptime.stop_id and StopTime.trip_id == stoptime.trip_id).first()
    if data is None:
        s.add(stoptime)


def add_transfer():
    pass


def add_trip(trip):
    data: Trip = s.query(Trip).filter(Trip.trip_id == trip.trip_id and Trip.route_id == trip.route_id).first()
    if data is None:
        s.add(trip)
    else:
        trip = data
    return trip

def new_session():
    global s
    s = Session()


def commit():
    s.commit()


def end_session():
    s.close()
