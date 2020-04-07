'''
Created on Apr 19, 2019

@author: afunes
'''

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import or_, and_, text

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept, RelCustomConceptConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.fileData import FileData
from modelClass.period import Period
from modelClass.ticker import Ticker


class CustomFactDao():
    
    def getCustomConcept(self, fillStrategy, session):
        #"COPY_CALCULATE"
        return GenericDao().getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == fillStrategy), session = session)
    
                            
    
    def getCustomConceptFilled(self, fileDataOID, session):
        params = { 'fileDataOID' : fileDataOID}
        query = text(""" select cc.conceptName
                            from fa_custom_concept cc
                                join fa_custom_fact cf on cc.oid = cf.customConceptOID
                                join fa_custom_fact_value cfv on cfv.customFactOID = cf.OID 
                             where fillStrategy = 'COPY_CALCULATE'
                                 and cfv.fileDataOID = :fileDataOID""")
        result = session.execute(query, params)
        returnResult = [x[0] for x in result]
        return returnResult
    
    
    def getCustomFactValue5(self, fillStrategy = '', ticker = '', session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFactValue.fileData)\
                .join(CustomFact.customConcept)\
                .join(FileData.company)\
                .join(Company.tickerList)\
                .filter(and_(or_(fillStrategy == '', CustomConcept.fillStrategy.__eq__(fillStrategy)), or_(ticker == '', Ticker.ticker.__eq__(ticker))))\
                .all()
            return objectResult
        except NoResultFound:
            return None
        
    def getCustomFact3(self, customConceptOID, customReportOID, session):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFact)\
                .filter(and_(CustomFact.customReportOID == customReportOID, CustomFact.customConceptOID == customConceptOID))\
                .one()
            return objectResult
        except NoResultFound:
            return None
        
    def getCustomFactValue4(self, companyOID, periodType, documentFiscalYearFocus, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(CustomFactValue)\
            .join(CustomFactValue.customFact)\
            .join(CustomFactValue.fileData)\
            .join(CustomFactValue.period)\
            .filter(and_(FileData.documentFiscalYearFocus == documentFiscalYearFocus, \
                         FileData.companyOID == companyOID, \
                         Period.type.__eq__(periodType)))\
            .with_entities(CustomFactValue.value, CustomFactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, CustomFactValue.fileDataOID, CustomFact.customConceptOID)\
            .order_by(FileData.documentPeriodEndDate)
        objectResult = query.all()
        return objectResult
    
    def deleteCFVByFD(self, fileDataOID, origin, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        session.query(CustomFactValue).filter(and_(CustomFactValue.fileDataOID==fileDataOID, CustomFactValue.origin == origin)).delete()
        
    def getCConceptAndConcept(self, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(CustomConcept)\
            .join(CustomConcept.relationConceptList)\
            .join(RelCustomConceptConcept.concept)\
            .join(CustomConcept.defaultCustomReport)\
            .with_entities(CustomReport.shortName,CustomConcept.conceptName.label("CustomConceptName"), Concept.conceptName, RelCustomConceptConcept.order_)\
            .order_by(CustomReport.shortName, CustomConcept.conceptName, RelCustomConceptConcept.order_)
        objectResult = query.all()
        return objectResult
