'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey

from modelClass import PersistenObject


class Ticker(PersistenObject):
    __tablename__ = 'fa_ticker'
    ticker = Column(String(10), nullable=False)
    tickerOrigin = Column(String(45), nullable=False)
    companyOID = Column(Integer, ForeignKey('fa_company.OID'))
    company = relationship("Company", back_populates="tickerList")
