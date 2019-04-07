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
copy = False
customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, session = session)

valuedCopied = 0
for customConcept in customConceptList:
    if(copy):
        valuedCopied += CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, session = session)
    
    CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptList, customConcept.conceptName, session)

