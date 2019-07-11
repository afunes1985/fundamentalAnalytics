'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import String, Integer

from modelClass import PersistenObject


class Expression(PersistenObject):
    __tablename__ = 'fa_expression'
    expression = Column(String(255), nullable=False)
    periodType = Column(String(4), nullable=False)
    customConceptOID = Column(Integer, ForeignKey('fa_custom_concept.OID'))
    customConcept = relationship("CustomConcept", back_populates="expressionList")
    defaultOrder = Column(Integer, nullable=False)