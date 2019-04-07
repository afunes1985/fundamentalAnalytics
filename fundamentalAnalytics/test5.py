'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from modelClass.customConcept import CustomConcept


Initializer()
session = DBConnector().getNewSession()

#customConceptName = 'COST_OF_REVENUE' 
ticker = 'INTC'
customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, session = session)

for customConcept in customConceptList:
    CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, 
                                     customReportName = 'CUSTOM_INCOME', defaultOrder = 99, session = session)
    
    #CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptList, customConcept.conceptName, session)

