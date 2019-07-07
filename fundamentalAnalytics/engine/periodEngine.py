'''
Created on Jul 6, 2019

@author: afunes
'''
from sqlalchemy.sql.expression import and_

from dao.dao import GenericDao
from dao.periodDao import PeriodDao
from modelClass.period import Period


class PeriodEngine():
    
    def getOrCreatePeriod(self, ticker, periodType, endDate, session):
        period = PeriodDao().getPeriodByFact3(ticker, periodType, endDate, session)
        if(period is None):
            period = GenericDao.getOneResult(Period, and_(Period.endDate == endDate, Period.startDate == None), session, raiseNoResultFound = False)
            if(period is None):
                period = Period()
                period.endDate = endDate
                period.type = 'QTD'
        return period