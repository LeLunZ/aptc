from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean, Interval

Base = declarative_base()


class Frequency(Base):
    __tablename__ = 'frequencies'
    trip_id = Column(Integer, nullable=False, primary_key=True)
    start_time = Column(Integer, nullable=False)
    end_time = Column(Interval, nullable=False)
    headway_secs = Column(Integer, nullable=False)
    exact_times = Column(Text, nullable=False)

    def __repr__(self):
        return "<Frequency(times='{}')>" \
            .format(self.exact_times)

    @staticmethod
    def firstline():
        return ['trip_id', 'start_time', 'end_time', 'headway_secs', 'exact_times']

    def tocsv(self):
        return [self.trip_id, self.start_time, self.end_time, self.headway_secs, self.exact_times]
