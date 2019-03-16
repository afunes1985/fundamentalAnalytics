'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class CustomFactValue(PersistenObject):
    __tablename__ = 'fa_custom_fact_value'
    periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    period = relationship("Period", back_populates="customFactValueList")
    customFactOID = Column(Integer, ForeignKey('fa_custom_fact.OID'))
    customFact = relationship("CustomFact", back_populates="customFactValueList")
    value = Column(Float(), nullable=False)
