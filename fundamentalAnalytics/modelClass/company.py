'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import relationship

from modelClass import PersistenObject

class Company(PersistenObject):
    __tablename__ = 'fa_company'
    CIK = Column(Integer, nullable=False)
    entityRegistrantName = Column(String(250), nullable=False)
    sector = Column(String(45), nullable=False)
    industry = Column(String(100), nullable=False)
    fileDataList = relationship("FileData", back_populates="company")
    listed = Column(Boolean, nullable=False)
    notListedDescription = Column(String(45), nullable=False)
    tickerList = relationship("Ticker", back_populates="company")
