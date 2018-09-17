'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class FactValue(PersistenObject):
    __tablename__ = 'fa_fact_value'
    periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    period = relationship("Period", back_populates="factValueList")
    factOID = Column(Integer, ForeignKey('fa_fact.OID'))
    fact = relationship("Fact", back_populates="factValueList")
    value = Column(Float(), nullable=False)
