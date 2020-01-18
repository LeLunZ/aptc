from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text

CalendarDateBase = declarative_base()


class CalendarDate(CalendarDateBase):
    __tablename__ = 'calendar_dates'
    service_id = Column(Integer, primary_key=True)
    date = Column(Integer, nullable=False, primary_key=True)
    exception_type = Column(Integer, nullable=False)

    def __repr__(self):
        return "<CalendarDate(name='{}', url='{}', id={})>" \
            .format(self.service_id, self.date, self.exception_type)

    @staticmethod
    def firstline():
        return ['service_id', 'date', 'exception_type']

    def tocsv(self):
        return [self.service_id, self.date, self.exception_type]
