'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import String

from modelClass import PersistenObject


class ExplicitMember(PersistenObject):
    __tablename__ = 'fa_explicit_member'
    explicitMemberValue = Column(String(100), nullable=False)
    order_ = Column(Integer, nullable=True)
    
    def __init__(self, explicitMemberValue, order_):
        self.explicitMemberValue = explicitMemberValue
        self.order_ = order_
