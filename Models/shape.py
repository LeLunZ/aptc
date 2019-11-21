from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Shape(Base):
    __tablename__ = 'shapes'
    shape_id = Column(Text)
    shape_pt_sequence = Column(Integer,nullable=False)
    shape_dist_traveled = Column(Float, nullable=True)
    shape_pt_lat = Column(Float, nullable=False)
    shape_pt_lon = Column(Float, nullable=False)

    def __repr__(self):
        return "<Shape(id='{}')>" \
            .format(self.shape_id)