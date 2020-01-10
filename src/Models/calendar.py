from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean, Numeric

Base = declarative_base()


class Calendar(Base):
    __tablename__ = 'calendar'
    service_id = Column(Text, primary_key=True)
    monday = Column(Boolean, nullable=False)
    tuesday = Column(Boolean, nullable=False)
    wednesday = Column(Boolean, nullable=False)
    thursday = Column(Boolean, nullable=False)
    friday = Column(Boolean, nullable=False)
    saturday = Column(Boolean, nullable=False)
    sunday = Column(Boolean, nullable=False)
    start_date = Column(Numeric, nullable=True)
    end_date = Column(Numeric, nullable=False)

    def __repr__(self):
        return "<Calendar(id='{}', start='{}', end={})>" \
            .format(self.service_id, self.start_date, self.end_date)

    @staticmethod
    def firstline():
        return ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                'start_date', 'end_date']

    def tocsv(self):
        if self.monday:
            monday = 1
        else:
            monday = 0
        if self.tuesday:
            tuesday = 1
        else:
            tuesday = 0
        if self.wednesday:
            wednesday = 1
        else:
            wednesday = 0
        if self.thursday:
            thursday = 1
        else:
            thursday = 0
        if self.friday:
            friday = 1
        else:
            friday = 0
        if self.saturday:
            saturday = 1
        else:
            saturday = 0
        if self.sunday:
            sunday = 1
        else:
            sunday = 0
        return [self.service_id, monday, tuesday, wednesday, thursday, friday, saturday,
                sunday, self.start_date, self.end_date]
