'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class CustomFact(PersistenObject):
    __tablename__ = 'fa_custom_fact'
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship('Company', back_populates="customFactList")
    customConceptOID = Column(Integer, ForeignKey('fa_custom_concept.OID'))
    customConcept = relationship("CustomConcept", back_populates="customFactList")
    customReportOID = Column(Integer, ForeignKey('fa_custom_report.OID'))
    customReport = relationship("CustomReport", back_populates="customFactList")
    customFactValueList = relationship("CustomFactValue", back_populates="customFact")
