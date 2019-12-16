from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Trip(Base):
    __tablename__ = 'trips'
    route_id = Column(Integer, nullable=False)
    service_id = Column(Text, nullable=True)
    trip_short_name = Column(Text, nullable=True)
    trip_headsign = Column(Text, nullable=True)
    route_short_name = Column(Text, nullable=True)
    direction_id = Column(Boolean, nullable=True)
    block_id = Column(Text, nullable=True)
    shape_id = Column(Text, nullable=True)
    wheelchair_accessible = Column(Text, nullable=True)
    trip_bikes_allowed = Column(Text, nullable=True)
    trip_id = Column(Integer, nullable=False, primary_key=True)

    def __repr__(self):
        return "<Trip(name='{}', id={})>" \
            .format(self.trip_short_name, self.trip_id)
