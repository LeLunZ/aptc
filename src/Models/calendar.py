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
    start_date = Column(Numeric, nullable=False)
    end_date = Column(Numeric, nullable=False)

    def __repr__(self):
        return "<Calendar(id='{}', start='{}', end={})>" \
            .format(self.service_id, self.start_date, self.end_date)