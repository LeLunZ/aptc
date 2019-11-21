from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Route(Base):
    __tablename__ = 'routes'
    agency_id = Column(Text, nullable=True)
    route_id = Column(Text, primary_key=True)
    route_short_name = Column(Text, nullable=False)
    route_long_name = Column(Text, nullable=False)
    route_desc = Column(Text, nullable=True)
    route_type = Column(Integer, nullable=True)
    route_url = Column(Text, nullable=True)
    route_color = Column(Text, nullable=True)
    route_text_color = Column(Text, nullable=True)
    route_bikes_allowed = Column(Text, nullable=True)

    def __repr__(self):
        return "<Route(name='{}', url='{}', id={})>" \
            .format(self.route_short_name, self.route_url, self.route_id)