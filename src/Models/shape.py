from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean

Base = declarative_base()


class Shape(Base):
    __tablename__ = 'shapes'
    shape_id = Column(Integer, primary_key=True, autoincrement=True)
    shape_pt_sequence = Column(Integer, nullable=False)
    shape_dist_traveled = Column(Float, nullable=True)
    shape_pt_lat = Column(Float, nullable=False)
    shape_pt_lon = Column(Float, nullable=False)

    def __repr__(self):
        return "<Shape(id='{}')>" \
            .format(self.shape_id)

    @staticmethod
    def firstline():
        return ['shape_id', 'shape_pt_sequence', 'shape_dist_traveled', 'shape_pt_lat', 'shape_pt_lon']

    def tocsv(self):
        return [self.shape_id, self.shape_pt_sequence, self.shape_dist_traveled, self.shape_pt_lat, self.shape_pt_lon]
