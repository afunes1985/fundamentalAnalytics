'''
Created on Apr 19, 2019

@author: afunes
'''
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_, text

from base.dbConnector import DBConnector
from dao.dao import GenericDao
from modelClass.company import Company
from modelClass.ticker import Ticker


class CompanyDao():
    
    def getCompany2(self, CIK, session):
        return GenericDao().getOneResult(Company,Company.CIK.__eq__(CIK), session, raiseNoResultFound = False)

    def getCompanyList(self, session = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            query = text("""select distinct c.CIK, c.entityRegistrantName, t.ticker
                                FROM fa_company c
                                    join fa_ticker t on t.companyOID = c.OID 
                                order by t.ticker""")
            rs = con.execute(query, [])
            return rs 
        
    def getCompanyListForReport(self, session = None):
        if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        query = session.query(Company)\
            .join(Company.tickerList)\
            .with_entities(Company.CIK, Company.entityRegistrantName, Company.listed, Company.notListedDescription,Ticker.ticker, Ticker.tickerOrigin, Ticker.active)
        objectResult = query.all()
        return objectResult
        
    
    def getCompanyListByTicker(self, tickerList, session = None):
        if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        query = session.query(Company)\
            .join(Company.tickerList)\
            .filter(and_(Ticker.ticker.in_(tickerList), 
                         Ticker.active == True))\
            .with_entities(Company.CIK, Company.entityRegistrantName, Ticker.ticker)
        objectResult = query.all()
        return objectResult
    
    def getTicker(self, ticker, session):
        return GenericDao().getOneResult(Ticker,Ticker.ticker.__eq__(ticker), session, raiseNoResultFound = False)
    
    def getTickerLike(self, tickerLike, session = None):
        if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        objectResult = session.query(Ticker)\
            .filter(Ticker.ticker.like(tickerLike))\
            .all()
        return objectResult
    
    def getAllTicker(self, session = None):
        if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        objectResult = session.query(Ticker)\
            .with_entities(Ticker.ticker)\
            .all()
        return objectResult