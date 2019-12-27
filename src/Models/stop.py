from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Stop(Base):
    __tablename__ = 'stops'
    stop_id = Column(Text, primary_key=True)
    stop_code = Column(Text, nullable=False, unique=True)
    stop_name = Column(Text, nullable=False)
    stop_desc = Column(Text, nullable=True)
    stop_lat = Column(Float, nullable=True)
    stop_lon = Column(Float, nullable=True) #TODO: Changeto False if needed
    zone_id = Column(Text, nullable=True)
    stop_url = Column(Text, nullable=True)
    location_type = Column(Integer, nullable=True)
    parent_station = Column(Text, nullable=True)
    wheelchair_boarding = Column(Text, nullable=True)
    stop_direction = Column(Text, nullable=True)

    def __repr__(self):
        return "<Stop(name='{}', url='{}', id={})>" \
            .format(self.stop_name, self.stop_url, self.stop_id)