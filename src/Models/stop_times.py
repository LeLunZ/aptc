from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean, Interval

Base = declarative_base()


class StopTime(Base):
    __tablename__ = 'stop_times'
    stop_times_id = Column(Integer, nullable=True, autoincrement=True)
    trip_id = Column(Integer, nullable=False, primary_key=True)
    stop_sequence = Column(Integer, nullable=False, primary_key=True)
    stop_id = Column(Integer, nullable=False, primary_key=True)
    arrival_time = Column(Interval, nullable=False)
    departure_time = Column(Interval, nullable=False)
    stop_headsign = Column(Text, nullable=True)
    pickup_type = Column(Integer, nullable=True)
    drop_off_type = Column(Integer, nullable=True)
    shape_dist_traveled = Column(Float, nullable=True)

    def __repr__(self):
        return "<Route(id='{}', arrival='{}', departure={})>"

    @staticmethod
    def firstline():
        return ['trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time', 'stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled']

    def tocsv(self):
        return [self.trip_id, self.stop_sequence, self.stop_id, self.arrival_time, self.departure_time,
                self.stop_headsign, self.pickup_type, self.drop_off_type,
                self.shape_dist_traveled]
