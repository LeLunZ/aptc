from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Route(Base):
    __tablename__ = 'routes'
    agency_id = Column(Integer, nullable=True)
    route_id = Column(Integer, primary_key=True, autoincrement=True)
    route_short_name = Column(Text, nullable=False)
    route_long_name = Column(Text, nullable=True)
    route_desc = Column(Text, nullable=True)
    route_type = Column(Integer, nullable=True)
    route_url = Column(Text, nullable=True)
    route_color = Column(Text, nullable=True)
    route_text_color = Column(Text, nullable=True)

    def __repr__(self):
        return "<Route(name='{}', url='{}', id={})>" \
            .format(self.route_short_name, self.route_url, self.route_id)

    @staticmethod
    def firstline():
        return ['agency_id', 'route_id', 'route_short_name', 'route_long_name', 'route_desc', 'route_type', 'route_url',
                'route_color', 'route_text_color']

    def tocsv(self):
        return [self.agency_id, self.route_id, self.route_short_name, self.route_long_name, self.route_desc,
                self.route_type, self.route_url, self.route_color, self.route_text_color]
