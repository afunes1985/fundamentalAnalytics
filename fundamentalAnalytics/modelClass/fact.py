'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey, Table

from modelClass import PersistenObject, Base


association_table = Table('fa_fact_report_relation', Base.metadata,
    Column('factOID', Integer, ForeignKey('fa_fact.OID')),
    Column('reportOID', Integer, ForeignKey('fa_report.OID'))
)

class Fact(PersistenObject):
    __tablename__ = 'fa_fact'
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="factList")
    reportOID = Column(Integer, ForeignKey('fa_report.OID'))
    report = relationship("Report", back_populates="factList")
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    fileData = relationship("FileData", back_populates="factList")
    factValueOID = Column(Integer, ForeignKey('fa_fact_value.OID'))
    factValue = relationship("FactValue", back_populates="factList")
    #factValueList = relationship("FactValue", back_populates="fact", cascade="all, delete-orphan")
    #order_ = Column(Integer, nullable=False)
    reportList = relationship("Report",
            secondary=association_table,
            backref="parents")
