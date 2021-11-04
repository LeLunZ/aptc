from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

from Scripts import primary_keys

AgencyBase = declarative_base()


class Agency(AgencyBase):
    __tablename__ = 'agency'
    agency_id = Column(Integer, primary_key=True, autoincrement=False)
    agency_name = Column(Text, nullable=True, unique=True)
    agency_url = Column(Text, nullable=True)
    agency_timezone = Column(Text, nullable=True)
    agency_lang = Column(Text, nullable=True)
    agency_phone = Column(Text, nullable=True)

    def __repr__(self):
        return "<Agency(name='{}', url='{}', id={})>" \
            .format(self.agency_name, self.agency_url, self.agency_id)

    def set_id(self):
        primary_keys.agency_id_count += 1
        self.agency_id = primary_keys.agency_id_count

    @staticmethod
    def firstline():
        return ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone']

    def tocsv(self):
        return [self.agency_id, self.agency_name, self.agency_url, self.agency_timezone, self.agency_lang,
                self.agency_phone]
