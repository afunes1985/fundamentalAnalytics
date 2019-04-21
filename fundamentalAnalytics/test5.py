'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from modelClass.company import Company
from modelClass.customConcept import CustomConcept


Initializer()
session = DBConnector().getNewSession()

#customConceptName = 'COST_OF_REVENUE' 
ticker = 'NFLX'
masive = True
copy = True
calculate = True
delete = False
if (masive):
    if(delete):
        CustomFactEngine.deleteCustomFactByCompany(ticker, session)
    customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, session = session)
    valuedCopied = 0
    for customConcept in customConceptList:
        if(copy):
            valuedCopied += CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, session = session)
        if(calculate):
            CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptName, session)
else:
    customConceptName = "LONG_TERM_INVESTMENTS"
    if(copy):
        CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
    if(calculate):
        CustomFactEngine.completeMissingQTDValues(ticker, customConceptName, session)
