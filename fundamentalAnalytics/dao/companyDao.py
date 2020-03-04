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
    
    def getTicker(self, ticker, session):
        return GenericDao().getOneResult(Ticker,Ticker.ticker.__eq__(ticker), session, raiseNoResultFound = False)