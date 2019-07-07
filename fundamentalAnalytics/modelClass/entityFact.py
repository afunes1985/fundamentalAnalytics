'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject

class EntityFact(PersistenObject):
    __tablename__ = 'fa_entity_fact'
    conceptOID = Column(Integer, ForeignKey('fa_concept.OID'))
    concept = relationship("Concept", back_populates="entityFactList")
    reportOID = Column(Integer, ForeignKey('fa_report.OID'))
    report = relationship("Report", back_populates="entityFactList")
    fileDataOID = Column(Integer, ForeignKey('fa_file_data.OID'))
    fileData = relationship("FileData", back_populates="entityFactList")
    entityFactValueList = relationship("EntityFactValue", back_populates="entityFact", cascade="all, delete-orphan")
    order_ = Column(Integer, nullable=False)
