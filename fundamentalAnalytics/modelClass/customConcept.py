'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Integer

from modelClass import PersistenObject


class CustomConcept(PersistenObject):
    __tablename__ = 'fa_custom_concept'
    conceptName = Column(String(250), nullable=False)
    label = Column(String(200), nullable=False)
    customFactList = relationship("CustomFact", back_populates="customConcept")
    defaultOrder = Column(Integer, nullable=False)
    defaultCustomReportOID = Column(Integer, ForeignKey('fa_custom_report.OID'))
    defaultCustomReport = relationship("CustomReport", back_populates="customConceptList")
