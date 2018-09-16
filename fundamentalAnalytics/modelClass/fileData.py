'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

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
