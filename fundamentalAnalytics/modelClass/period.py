'''
Created on 26 ago. 2018

@author: afunes
'''
from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Integer, DateTime, String

from modelClass import PersistenObject


class Period(PersistenObject):
    __tablename__ = 'fa_period'
    startDate = Column(DateTime, nullable=False)
    endDate = Column(DateTime, nullable=False)
    instant = Column(DateTime, nullable=False)
    type = Column(String(4), nullable=False)
    factValueList = relationship("FactValue", back_populates="period")
    customFactValueList = relationship("CustomFactValue", back_populates="period")
    
    def getKeyDate(self):
        if(self.type == "QTD" or self.type == "YTD"):
            return self.endDate
        elif(type == "INST"):
            return self.instant
            

class QuarterPeriod(PersistenObject):
    __tablename__ = 'fa_quarter_period'
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
