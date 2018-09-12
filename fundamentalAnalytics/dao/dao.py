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
    
    @staticmethod
    def getAllResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .all()
        return objectResult

class DaoCompanyResult():
    
    @staticmethod
    def getCompanyResult(companyID = None, ticker = None, conceptID = None, sectionID = None, periodType = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptID' : conceptID,
                       'companyID' : companyID,
                       'ticker' : ticker,
                       'sectionID' : sectionID,
                       'periodType' : periodType}
            query = text("""select report.shortName, concept.conceptID, concept.label, fact.value, IFNULL(period.endDate, period.instant)
    FROM fa_fact fact
        left join fa_company company on fact.companyOID = company.OID
        left join fa_concept concept on fact.conceptOID = concept.OID
        left join fa_period period on fact.periodOID = period.OID
        left join fa_report report on fact.reportOID = report.OID
    #where (concept.conceptID = :conceptID or :conceptID is null) 
        #and (company.CIK = :companyID or :companyID is null)
        #and (company.ticker = :ticker or :ticker is null)
        #and (report.shortName = :reportShortName or :reportShortName is null)
    order by report.shortName, concept.conceptID""")
            rs = con.execute(query, params)
            return rs 
    
