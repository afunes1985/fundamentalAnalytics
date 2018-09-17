'''
Created on 26 ago. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Integer, DateTime

from modelClass import PersistenObject


class Period(PersistenObject):
    __tablename__ = 'fa_period'
    startDate = Column(DateTime, nullable=False)
    endDate = Column(DateTime, nullable=False)
    instant = Column(DateTime, nullable=False)
    factValueList = relationship("FactValue", back_populates="period")

class QuarterPeriod(PersistenObject):
    __tablename__ = 'fa_quarter_period'
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
