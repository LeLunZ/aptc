from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Transfer(Base):
    __tablename__ = 'transfers'
    from_stop_id = Column(Integer, nullable=False, primary_key=True)
    to_stop_id = Column(Integer, nullable=False, primary_key=True)
    transfer_type = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Transfer(transfer_type='{}')>" \
            .format(self.transfer_type)

    @staticmethod
    def firstline():
        return ['from_stop_id', 'to_stop_id', 'transfer_type']

    def tocsv(self):
        return [self.from_stop_id, self.to_stop_id, self.transfer_type]
