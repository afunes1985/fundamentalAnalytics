'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject, Base

class RelFactReport(Base):
    __tablename__ = 'fa_fact_report_relation'
    factOID = Column(Integer, ForeignKey('fa_fact.OID'), primary_key=True)
    reportOID = Column(Integer, ForeignKey('fa_report.OID'), primary_key=True)
    order_ = Column(Integer, nullable=True)

class Fact(PersistenObject):
    __tablename__ = 'fa_fact'
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="factList")
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    fileData = relationship("FileData", back_populates="factList")
    factValueOID = Column(Integer, ForeignKey('fa_fact_value.OID'))
    factValue = relationship("FactValue", back_populates="factList")
    relationReportList = relationship("RelFactReport")
