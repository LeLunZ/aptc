from Models.agency import *

DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/postgres'

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)

s = Session()


def add_agency():
    pass


def add_calendar():
    pass


def add_frequency():
    pass


def add_route():
    pass


def add_shape():
    pass


def add_stop():
    pass


def add_stop_time():
    pass


def add_transfer():
    pass


def add_trip():
    pass


def new_session():
    global s
    s = Session()


def commit():
    s.commit()


def end_session():
    s.close()
