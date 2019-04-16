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
ticker = 'CSCO'
masive = True
copy = True
calculate = True
if (masive):
    customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, session = session)
    valuedCopied = 0
    for customConcept in customConceptList:
        if(copy):
            valuedCopied += CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, session = session)
        if(calculate):
            CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptName, session)
else:
    customConceptName = "GAIN_LOSS_SALE_ASSETS"
    if(copy):
        CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
    if(calculate):
        CustomFactEngine.completeMissingQTDValues(ticker, customConceptName, session)
