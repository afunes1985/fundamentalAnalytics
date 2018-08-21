'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy import Column, Integer, String, Float

Base = declarative_base()

class CompanyQFundamental(Base):
    __tablename__ = 'company_q_fundamental'
    id = Column(Integer, primary_key=True)
    companyID = Column(Integer, nullable=False)
    indicatorID = Column(String(200), nullable=False)
    q_2011Q2 = Column(Float(), nullable=False)
    q_2011Q3 = Column(Float(), nullable=False)
    q_2011Q4 = Column(Float(), nullable=False)
    q_2012Q1 = Column(Float(), nullable=False)
    q_2012Q2 = Column(Float(), nullable=False) 
    q_2012Q3 = Column(Float(), nullable=False)
    q_2012Q4 = Column(Float(), nullable=False)
    q_2013Q1 = Column(Float(), nullable=False)
    q_2013Q2 = Column(Float(), nullable=False)
    q_2013Q3 = Column(Float(), nullable=False)
    q_2013Q4 = Column(Float(), nullable=False)
    q_2014Q1 = Column(Float(), nullable=False)
    q_2014Q2 = Column(Float(), nullable=False)
    q_2014Q3 = Column(Float(), nullable=False)
    q_2014Q4 = Column(Float(), nullable=False)
    q_2015Q1 = Column(Float(), nullable=False)
    q_2015Q2 = Column(Float(), nullable=False)
    q_2015Q3 = Column(Float(), nullable=False)
    q_2015Q4 = Column(Float(), nullable=False)
    q_2016Q1 = Column(Float(), nullable=False)
    q_2016Q2 = Column(Float(), nullable=False)
    q_2016Q3 = Column(Float(), nullable=False)
    q_2016Q4 = Column(Float(), nullable=False)
    q_2017Q1 = Column(Float(), nullable=False)
    q_2017Q2 = Column(Float(), nullable=False)
    q_2017Q3 = Column(Float(), nullable=False)
    q_2017Q4 = Column(Float(), nullable=False)
    q_2018Q1 = Column(Float(), nullable=False)
    q_2018Q2 = Column(Float(), nullable=False)