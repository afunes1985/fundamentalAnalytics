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
masive = False
if (masive):
    copy = True
    calculate = False
    customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, session = session)
    valuedCopied = 0
    for customConcept in customConceptList:
        if(copy):
            valuedCopied += CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, session = session)
        if(calculate):
            CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptList, customConcept.conceptName, session)
else:
    customConceptName = "DEPRECIATION_AMORTIZATION_AND_OTHER"
    CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)

