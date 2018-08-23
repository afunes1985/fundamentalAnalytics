'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class Concept(Base):
    __tablename__ = 'fa_concept'
    id = Column(Integer, primary_key=True)
    section = Column(String(45), nullable=False)
    indicatorID = Column(String(250), nullable=False)
    label = Column(String(200), nullable=False)
