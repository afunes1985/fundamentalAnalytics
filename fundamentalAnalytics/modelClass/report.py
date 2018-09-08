'''
Created on 7 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from modelClass import PersistenObject


class Report(PersistenObject):
    __tablename__ = 'fa_report'
    shortName = Column(String(250), nullable=False)
    factList = relationship("Fact", back_populates="report")
