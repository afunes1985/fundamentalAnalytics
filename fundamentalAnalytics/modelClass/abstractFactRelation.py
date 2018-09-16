'''
Created on 12 sep. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Integer

from modelClass import PersistenObject


class AbstractFactRelation(PersistenObject):
    __tablename__ = 'fa_abstract_fact_relation'
    abstractFromOID = Column(Integer, ForeignKey('fa_abstract_concept.OID'))
    abstractConceptFrom = relationship('AbstractConcept', back_populates="abstractFactRelationList")
    abstractToOID = Column(Integer, nullable=True)
    factToOID = Column(Integer, nullable=True)
