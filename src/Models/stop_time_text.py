from sqlalchemy import Column, Text
from sqlalchemy.ext.declarative import declarative_base

StopTimeTextBase = declarative_base()


class StopTimeText(StopTimeTextBase):
    __tablename__ = 'stop_time_text'
    working_days = Column(Text, primary_key=True)
