'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class Fact(PersistenObject):
    __tablename__ = 'fa_fact'
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship('Company', back_populates="factList")
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="factList")
    #periodOID = Column(Integer, ForeignKey('fa_period.OID'))
    #period = relationship("Period", back_populates="factList")
    reportOID = Column(Integer, ForeignKey('fa_report.OID'))
    report = relationship("Report", back_populates="factList")
    value = Column(Float(), nullable=False)
