'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from modelClass import PersistenObject

class Company(PersistenObject):
    __tablename__ = 'fa_company'
    CIK = Column(Integer, nullable=False)
    name = Column(String(250), nullable=False)
    ticker = Column(String(45), nullable=False)
    sector = Column(String(45), nullable=False)
    industry = Column(String(100), nullable=False)
    factList = relationship("Fact", back_populates="company")
