from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text

StopTimeTextBase = declarative_base()


class StopTimeText(StopTimeTextBase):
    __tablename__ = 'stop_time_text'
    working_days = Column(Text, primary_key=True)
