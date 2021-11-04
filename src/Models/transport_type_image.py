from sqlalchemy import Column, Text
from sqlalchemy.ext.declarative import declarative_base

TransportTypeImageBase = declarative_base()


class TransportTypeImage(TransportTypeImageBase):
    __tablename__ = 'transport_type_image'
    name = Column(Text, primary_key=True)
    oebb_url = Column(Text)
