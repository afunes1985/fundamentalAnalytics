'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from modelClass import PersistenObject


class Concept(PersistenObject):
    __tablename__ = 'fa_concept'
    conceptID = Column(String(250), nullable=False)
    label = Column(String(200), nullable=False)
    section = relationship("Section", back_populates="conceptList")
    companyQResultList = relationship("CompanyQResult", back_populates="concept")
