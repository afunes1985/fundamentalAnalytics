'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Boolean, Integer

from modelClass import PersistenObject


class FileData(PersistenObject):
    __tablename__ = 'fa_file_data'
    fileName = Column(String(45), nullable=False)
    documentType = Column(String(4), nullable=False)
    amendmentFlag = Column(Boolean, nullable=False)
    documentPeriodEndDate = Column(String(45), nullable=False)
    documentFiscalYearFocus = Column(String(45), nullable=False)
    documentFiscalPeriodFocus = Column(String(45), nullable=False)
    entityCentralIndexKey = Column(String(45), nullable=False)
    factList = relationship("Fact", back_populates="fileData")
    entityFactList = relationship("EntityFact", back_populates="fileData")
    customFactList = relationship("CustomFact", back_populates="fileData")
    errorMessageList = relationship("ErrorMessage", back_populates="fileData", cascade="all, delete-orphan")
    status = Column(String(15), nullable=False)
    importStatus = Column(String(15), nullable=False)
    entityStatus = Column(String(15), nullable=False)
    priceStatus = Column(String(15), nullable=False)
    copyStatus = Column(String(15), nullable=False)
    CIK = Column(Integer, nullable=False)
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship('Company', back_populates="fileDataList")