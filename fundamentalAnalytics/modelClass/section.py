'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Integer
from modelClass import PersistenObject


class Section(PersistenObject):
    __tablename__ = 'fa_section'
    section = Column(String(45), nullable=False)
    subSection = Column(String(45), nullable=False)
    currentNonCurrent = Column(String(45), nullable=False)
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    conceptList = relationship("Concept", back_populates="section")