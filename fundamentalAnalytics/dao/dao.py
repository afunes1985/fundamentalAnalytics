'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.sql.expression import text

from base.dbConnector import DBConnector

class GenericDao():
    @staticmethod
    def getFirstResult(objectClazz, attributeClazz, value, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(attributeClazz.__eq__(value))\
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
                                inner join fa_company c on cqr.companyID = c.companyID
                            where (cqr.indicatorID = :indicatorID or :indicatorID is null) 
                                and (cqr.companyID = :companyID or :companyID is null)
                                and (c.ticker = :ticker or :ticker is null)
                            order by year, quarter""")
            rs = con.execute(query, params)
            return rs 
    
