'''
Created on Jul 6, 2019

@author: afunes
'''
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from modelClass.company import Company
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period

class PeriodDao():
    
    def getPeriodByFact3(self, ticker, periodType, endDate, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(Period)\
                .join(Period.factValueList)\
                .join(FactValue.fact)\
                .join(Fact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType), Period.endDate == endDate))
            objectResult = query.one()
            return objectResult
        except NoResultFound:
            return None   