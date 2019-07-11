'''
Created on Jul 6, 2019

@author: afunes
'''
from sqlalchemy.sql.expression import and_

from dao.dao import GenericDao, Dao
from dao.periodDao import PeriodDao
from modelClass.period import Period


class PeriodEngine():
    
    def getOrCreatePeriod(self, periodType, endDate, session):
        period = GenericDao().getOneResult(Period, and_(Period.endDate == endDate, Period.startDate == None), session, raiseNoResultFound = False)
        if(period is None):
            period = Period()
            period.endDate = endDate
            period.type = periodType
        return period
    
    def getOrCreatePeriod2(self, startDate, endDate, session):
        period =  GenericDao().getOneResult(Period, and_(Period.startDate == startDate, Period.endDate == endDate), session, raiseNoResultFound = False)
        if (period is None):
            period = Period()
            period.startDate = startDate
            period.endDate = endDate
            period.type = self.getPeriodType(startDate, endDate)
            #Dao().addObject(objectToAdd = period, session = session, doFlush = True)
        return period
    
    def getOrCreatePeriod3(self, instant, session):
        period =  GenericDao().getOneResult(Period, and_(Period.instant == instant), session, raiseNoResultFound = False)
        if (period is None):
            period = Period()
            period.instant = instant
            period.type = "INST"
            #Dao().addObject(objectToAdd = period, session = session, doFlush = True)
        return period
    
    def getPeriodType(self, startDate, endDate):
        days = self.getDaysBetweenDates(startDate, endDate)
        if(70 < days < 110):
            return "QTD"
        elif(days > 130):
            return "YTD"
    
    def getDaysBetweenDates(self, firstDate, secondDate):
        if(secondDate is not None and firstDate is not None):
            return abs((secondDate - firstDate).days)
        else:
            return 10000
        