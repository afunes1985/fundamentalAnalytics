'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import text, and_

from base.dbConnector import DBConnector
from modelClass.concept import Concept
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.report import Report


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
    
class Dao():
    @staticmethod
    def getFactValue(fact, period, session):
        try:
            return GenericDao.getOneResult(FactValue, and_(FactValue.fact.__eq__(fact), FactValue.period.__eq__(period)), session)
        except NoResultFound:
            return FactValue()

    @staticmethod
    def getConcept(conceptName, session):
        try:
            return GenericDao.getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session)
        except NoResultFound:
            concept = Concept()
            concept.conceptName = conceptName
            session.add(concept)
            session.flush()
            logging.getLogger('addToDB').debug("Added concept" + conceptName)
            return concept
    
    @staticmethod
    def getFact(company, concept, report, fileData, session):
        try:
            return GenericDao.getOneResult(Fact, and_(Fact.company == company, Fact.concept == concept, Fact.report == report, fileData == fileData), session)
        except NoResultFound:
            return Fact()
        
    @staticmethod  
    def getReport(reportShortName, session):
        try:
            return GenericDao.getOneResult(Report, and_(Report.shortName == reportShortName), session)
        except NoResultFound:
            report = Report()
            report.shortName = reportShortName
            session.add(report)
            session.flush()
            logging.getLogger('addToDB').debug("ADDED report " + reportShortName)
            return report