'''
Created on Apr 19, 2019

@author: afunes
'''

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import or_, and_

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept, RelCustomConceptConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.fileData import FileData
from modelClass.period import Period


class CustomFactDao():
    
    def getCustomConcept(self, fillStrategy, session):
        #"COPY_CALCULATE"
        return GenericDao().getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == fillStrategy), session = session)
    
    @staticmethod
    def getCustomFactValue(ticker, customConceptName, periodType = None, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            query = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFactValue.fileData)\
                .join(FileData.company)\
                .join(CustomFactValue.period)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName), Period.type.__eq__(periodType)))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None

    @staticmethod
    def getCustomFactValue2(ticker, customConceptName, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            query = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFactValue.fileData)\
                .join(FileData.company)\
                .join(CustomFactValue.period)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName)))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
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
                .filter(and_(or_(fillStrategy == '', CustomConcept.fillStrategy.__eq__(fillStrategy)), or_(ticker == '', Company.ticker.__eq__(ticker))))\
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
        
    def getCustomFactValue3(self, ticker, customConceptName, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFactValue.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker == ticker, CustomConcept.conceptName == customConceptName))\
                .all()
            return objectResult
        except NoResultFound:
            return None
        
    def getCustomFactValue4(self, companyOID, documentFiscalYearFocus, customConceptOID, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(CustomFactValue)\
            .join(CustomFactValue.customFact)\
            .join(CustomFactValue.fileData)\
            .filter(and_(FileData.documentFiscalYearFocus == documentFiscalYearFocus, CustomFact.customConceptOID == customConceptOID, FileData.companyOID == companyOID))\
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
            .with_entities(CustomConcept.conceptName.label("CustomConceptName"), Concept.conceptName, RelCustomConceptConcept.order_)\
            .order_by(CustomConcept.conceptName, RelCustomConceptConcept.order_)
        objectResult = query.all()
        return objectResult
