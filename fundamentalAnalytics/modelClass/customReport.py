'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from modelClass import PersistenObject


class CustomReport(PersistenObject):
    __tablename__ = 'fa_custom_report'
    shortName = Column(String(250), nullable=False)
    customFactList = relationship("CustomFact", back_populates="customReport")
    customConceptList = relationship("CustomConcept", back_populates="defaultCustomReport")
