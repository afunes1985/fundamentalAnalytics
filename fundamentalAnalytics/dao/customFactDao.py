'''
Created on Apr 19, 2019

@author: afunes
'''

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_

from base.dbConnector import DBConnector
from dao.dao import Dao
from modelClass.company import Company
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.factValue import FactValue
from modelClass.period import Period


class CustomFactDao():
    
    @staticmethod
    def getCustomFact2(ticker, customConceptName, session):
        try:
            company = Dao.getCompany(ticker, session)
            if (company is None):
                raise Exception('Company not found ' + ticker )
            customConcept = Dao.getCustomConcept(customConceptName, session)
            if (customConcept is None):
                raise Exception('CustomConcept not found ' + ticker )
            return Dao.getCustomFact(company, customConcept, customConcept.defaultCustomReport, session)
        except NoResultFound:
            return None
        
    @staticmethod
    def getCustomFactValue(ticker, customConceptName, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.company)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName), Period.type.__eq__(periodType)))\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
        
    @staticmethod
    def getCustomFact(fillStrategy, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFact)\
                .join(CustomFact.customConcept)\
                .filter(and_(CustomConcept.fillStrategy.__eq__(fillStrategy)))\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
    