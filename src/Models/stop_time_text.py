from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text

StopTimeTextBase = declarative_base()


class StopTimeText(StopTimeTextBase):
    __tablename__ = 'stop_time_text'
    split_traffic_day = Column(Text, primary_key=True)
    traffic_day = Column(Text, primary_key=True)
    extra_info_traffic_day = Column(Text, primary_key=True)
