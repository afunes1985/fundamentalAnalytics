'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import String

from modelClass import PersistenObject


class Expression(PersistenObject):
    __tablename__ = 'fa_expression'
    name = Column(String(255), nullable=False)
    expression = Column(String(255), nullable=False)
    periodType = Column(String(4), nullable=False)