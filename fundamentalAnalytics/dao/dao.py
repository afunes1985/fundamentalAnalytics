'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.sql.expression import text

from base.dbConnector import DBConnector

class GenericDao():
    @staticmethod
    def getFirstResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .first()
        return objectResult
    
    @staticmethod
    def getOneResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .one()
        return objectResult

class DaoCompanyResult():
    
    @staticmethod
    def getCompanyResult(companyID, ticker, indicatorID):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'indicatorID' : indicatorID,
                       'companyID' : companyID,
                       'ticker' : ticker}
            query = text("""select CONCAT(year, 'Q', quarter) as yq, value 
                            FROM fa_company_q_result cqr
                                inner join fa_company company on cqr.companyOID = company.OID
                                inner join fa_concept concept on cqr.conceptOID = concept.OID
                                inner join fa_period period on cqr.periodOID = period.OID
                            where (concept.conceptID = :indicatorID or :indicatorID is null) 
                                and (company.CIK = :companyID or :companyID is null)
                                and (company.ticker = :ticker or :ticker is null)
                            order by year, quarter""")
            rs = con.execute(query, params)
            return rs 
    
