from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text

TransportTypeImageBase = declarative_base()


class TransportTypeImage(TransportTypeImageBase):
    __tablename__ = 'transport_type_image'
    name = Column(Text, primary_key=True)
    oebb_url = Column(Text)