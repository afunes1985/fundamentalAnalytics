'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import  and_

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept
from modelClass.customReport import CustomReport
from modelClass.factValue import FactValue
from modelClass.report import Report
from valueobject.constant import Constant


class GenericDao():
    
    def getFirstResult(self, objectClazz, condition, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .first()
        return objectResult
    
    def getOneResult(self, objectClazz, condition =  (1 == 1), session = None, raiseNoResultFound = True):
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
    
    def getAllResult(self, objectClazz, condition = (1 == 1), session = None, limit = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .limit(limit)\
        .all()
        return objectResult


class Dao():
    
    def addObjectList(self, objectList, session):
        if(len(objectList) > 0):
            for obj in objectList:
                Dao().addObject(objectToAdd=obj, session=session) 
            session.commit()
    
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
    
    def getCustomConcept(self, customConceptName, session = None):
        return GenericDao().getOneResult(CustomConcept, CustomConcept.conceptName.__eq__(customConceptName), session, raiseNoResultFound = False)

    def getCustomReport(self, reportShortName, session = None):
        return GenericDao().getOneResult(CustomReport, and_(CustomReport.shortName == reportShortName), session, raiseNoResultFound = False)
