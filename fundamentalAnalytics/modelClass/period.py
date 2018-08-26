'''
Created on 26 ago. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Integer

from modelClass import PersistenObject


class Period(PersistenObject):
    __tablename__ = 'fa_period'
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    companyQResultList = relationship("CompanyQResult", back_populates="period")