from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Trip(Base):
    __tablename__ = 'trips'
    route_id = Column(Integer, nullable=False)
    service_id = Column(Integer, nullable=True)
    trip_short_name = Column(Text, nullable=True)
    trip_headsign = Column(Text, nullable=True)
    direction_id = Column(Boolean, nullable=True)
    block_id = Column(Text, nullable=True)
    shape_id = Column(Text, nullable=True)
    wheelchair_accessible = Column(Text, nullable=True)
    trip_id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    oebb_url = Column(Text, nullable=True)

    def __repr__(self):
        return "<Trip(name='{}', id={})>" \
            .format(self.trip_short_name, self.trip_id)

    @staticmethod
    def firstline():
        return ['route_id', 'service_id', 'trip_short_name', 'trip_headsign', 'direction_id',
                'block_id', 'shape_id', 'wheelchair_accessible', 'trip_id']

    def tocsv(self):
        return [self.route_id, self.service_id, self.trip_short_name, self.trip_headsign,
                self.direction_id, self.block_id, self.shape_id, self.wheelchair_accessible,
                self.trip_id]
