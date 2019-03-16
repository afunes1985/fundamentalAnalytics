'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class Fact(PersistenObject):
    __tablename__ = 'fa_fact'
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship('Company', back_populates="factList")
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="factList")
    reportOID = Column(Integer, ForeignKey('fa_report.OID'))
    report = relationship("Report", back_populates="factList")
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    fileData = relationship("FileData", back_populates="factList")
    factValueList = relationship("FactValue", back_populates="fact")
    order = Column(Integer, nullable=False)
