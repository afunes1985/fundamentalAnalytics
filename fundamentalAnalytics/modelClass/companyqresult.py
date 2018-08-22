'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy import Column, Integer, String, Float

Base = declarative_base()

class CompanyQResult(Base):
    __tablename__ = 'fa_company_q_result'
    id = Column(Integer, primary_key=True)
    companyID = Column(Integer, nullable=False)
    indicatorID = Column(String(200), nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    value = Column(Float(), nullable=False)