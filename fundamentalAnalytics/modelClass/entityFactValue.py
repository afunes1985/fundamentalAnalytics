'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class EntityFactValue(PersistenObject):
    __tablename__ = 'fa_entity_fact_value'
    periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    period = relationship("Period", back_populates="entityFactValueList")
    entityFactOID = Column(Integer, ForeignKey('fa_entity_fact.OID'))
    entityFact = relationship("EntityFact", back_populates="entityFactValueList")
    value = Column(Float(), nullable=False)
