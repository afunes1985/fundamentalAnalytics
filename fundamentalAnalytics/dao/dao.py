'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import text, and_

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.expression import Expression
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period
from modelClass.report import Report
from valueobject.constant import Constant


class GenericDao():
    @staticmethod
    def getFirstResult(objectClazz, condition, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .first()
        return objectResult
    
    def getOneResult(self, objectClazz, condition = "", session = None, raiseNoResultFound = True):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        try:
            objectResult = session.query(objectClazz)\
            .filter(condition)\
            .one()
        except NoResultFound as e:
            if(raiseNoResultFound):
                raise e
            return None
        return objectResult
    
    @staticmethod
    def getAllResult(objectClazz, condition = (1 == 1), session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .all()#.limit(100)\
        return objectResult


class Dao():
    
    @staticmethod
    def getFactValue(fact, period, session):
        return GenericDao().getOneResult(FactValue, and_(FactValue.fact.__eq__(fact), FactValue.period.__eq__(period)), session, raiseNoResultFound = False)
        
    def getConcept(self, conceptName, session = None):
        return GenericDao().getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session, raiseNoResultFound = False)
    
    def getReport(self, reportShortName, session):
        return GenericDao().getOneResult(Report, and_(Report.shortName == reportShortName), session, raiseNoResultFound = False)
    
    def addObject(self, objectToAdd, session = None, doCommit = False, doFlush = False):
        if(session is None):
            internalSession = DBConnector().getNewSession()
        else:
            internalSession = session
        internalSession.add(objectToAdd)
        if(doCommit):
            internalSession.commit()
        elif(doFlush):
            internalSession.flush()
        if(session is None):
            internalSession.close()
        logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added to DB " + str(objectToAdd))

    @staticmethod     
    def addAbstractConcept(factVO, session):
        try:
            abstractConcept =  GenericDao().getOneResult(AbstractConcept, and_(AbstractConcept.conceptName == factVO.conceptName), session)
        except NoResultFound:
            abstractConcept = AbstractConcept()
            abstractConcept.conceptName = factVO.conceptName
            Dao().addObject(objectToAdd = abstractConcept, session = session, doCommit = False)
        factVO.abstractConcept = abstractConcept
        return factVO
    
    @staticmethod
    def getCustomConcept(customConceptName, session = None):
        return GenericDao().getOneResult(CustomConcept, CustomConcept.conceptName.__eq__(customConceptName), session, raiseNoResultFound = False)

    @staticmethod  
    def getCustomReport(reportShortName, session = None):
        return GenericDao().getOneResult(CustomReport, and_(CustomReport.shortName == reportShortName), session, raiseNoResultFound = False)
    
    @staticmethod
    def getCustomFact2(ticker, customConcept, session):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(CustomFact)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .join(CustomFact.customConcept)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName == customConcept))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
    
    @staticmethod
    def getExpression(expressionName, session = None):
        return GenericDao().getOneResult(Expression, Expression.name == expressionName, session, raiseNoResultFound=False)
    
    @staticmethod    
    def getPeriodByFact(ticker, conceptName, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(Period)\
                .join(Period.factValueList)\
                .join(FactValue.fact)\
                .join(Fact.concept)\
                .join(Fact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType), \
                             Concept.conceptName.__eq__(conceptName)))\
                .order_by(Period.endDate)\
                .with_entities(Period.OID, Period.endDate, Fact.fileDataOID)\
                .distinct()
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
        
    @staticmethod    
    def getPeriodByFact2(ticker, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(Period)\
                .join(Period.factValueList)\
                .join(FactValue.fact)\
                .join(Fact.concept)\
                .join(Fact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType)))\
                .order_by(Period.endDate)\
                .with_entities(Period.OID, Period.endDate)\
                .distinct()
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
       

        
    @staticmethod    
    def getPeriodByCustomFact(ticker, conceptName, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(Period)\
                .join(Period.customFactValueList)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType), CustomConcept.conceptName.__eq__(conceptName)))\
                .order_by(Period.endDate)\
                .with_entities(Period.OID, Period.endDate)
            #print(str(query))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return FactValue()
        
