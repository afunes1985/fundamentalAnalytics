'''
Created on 12 sep. 2018

@author: afunes
'''
from sqlalchemy import Column, String
from modelClass import PersistenObject

class AbstractConcept(PersistenObject):
    __tablename__ = 'fa_abstract_concept'
    conceptName = Column(String(250), nullable=False)
    label = Column(String(200), nullable=True)
