from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean, Interval

Base = declarative_base()


class StopTime(Base):
    __tablename__ = 'stop_times'
    trip_id = Column(Integer, nullable=False, primary_key=True)
    stop_sequence = Column(Integer, nullable=False)
    stop_id = Column(Text, nullable=False, primary_key=True)
    arrival_time = Column(Interval, nullable=False)
    departure_time = Column(Interval, nullable=False)
    stop_headsign = Column(Text, nullable=True)
    route_short_name = Column(Text, nullable=True)
    pickup_type = Column(Integer, nullable=True)
    drop_off_type = Column(Integer, nullable=True)
    shape_dist_traveled = Column(Float, nullable=True)

    def __repr__(self):
        return "<Route(route_id='{}', arrival='{}', departure={})>" \
            .format(self.stop_id, self.arrival_time, self.departure_time)
