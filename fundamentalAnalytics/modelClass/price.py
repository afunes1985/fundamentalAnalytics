'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Float

from modelClass import PersistenObject


class Price(PersistenObject):
    __tablename__ = 'fa_price'
    periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    period = relationship("Period", back_populates="priceList")
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    #fileData = relationship("FileData", back_populates="entityFactList")
    value = Column(Float(), nullable=False)
