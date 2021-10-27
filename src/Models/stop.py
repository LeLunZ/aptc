from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Stop(Base):
    __tablename__ = 'stops'
    stop_id = Column(Integer, primary_key=True, autoincrement=True)
    stop_code = Column(Text, nullable=False)
    stop_name = Column(Text, nullable=False)
    stop_desc = Column(Text, nullable=True)
    stop_lat = Column(Float, nullable=True)
    stop_lon = Column(Float, nullable=True)
    zone_id = Column(Text, nullable=True)
    stop_url = Column(Text, nullable=True)
    location_type = Column(Integer, nullable=False)
    parent_station = Column(Integer, nullable=True)
    wheelchair_boarding = Column(Text, nullable=True)
    crawled = Column(Boolean, default=False)
    input = Column(Text, nullable=True)
    ext_id = Column(Integer, nullable=True, unique=True)
    prod_class = Column(Integer, nullable=True)
    siblings_searched = Column(Boolean, nullable=False, default=False)

    def __repr__(self):
        return "<Stop(name='{}', url='{}', id={})>" \
            .format(self.stop_name, self.stop_url, self.stop_id)

    @staticmethod
    def firstline():
        return ['stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'zone_id', 'stop_url',
                'location_type', 'parent_station', 'wheelchair_boarding']

    def tocsv(self):
        return [self.stop_id, self.stop_code, self.stop_name, self.stop_desc, self.stop_lat, self.stop_lon,
                self.zone_id, self.stop_url, self.location_type, self.parent_station, self.wheelchair_boarding]
