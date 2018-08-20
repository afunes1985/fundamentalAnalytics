'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
 
Base = declarative_base()
 
class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    companyID = Column(String(45), nullable=False)
    name = Column(String(250), nullable=False)
    ticker = Column(String(45), nullable=False)
    sector = Column(String(45), nullable=False)