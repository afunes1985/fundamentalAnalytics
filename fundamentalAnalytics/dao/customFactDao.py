'''
Created on Apr 19, 2019

@author: afunes
'''

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import or_

from base.dbConnector import DBConnector
from dao.dao import Dao
from modelClass.company import Company
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period


class CustomFactDao():
    
    @staticmethod
    def getCustomFactValue(ticker, customConceptName, periodType = None, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            query = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .join(CustomFactValue.period)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName), Period.type.__eq__(periodType)))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return FactValue()

    @staticmethod
    def getCustomFactValue2(ticker, customConceptName, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            query = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .join(CustomFactValue.period)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName)))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return FactValue()
        
    def getCustomFact(self, fillStrategy = '', ticker = '', session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .filter(or_(and_(fillStrategy != ''), CustomConcept.fillStrategy.__eq__(fillStrategy)), and_(ticker != '', Company.ticker.__eq__(ticker)))\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
    
    def getCustomFact3(self, ticker, customConceptName, session = None):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker == ticker, CustomConcept.conceptName == customConceptName))\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()