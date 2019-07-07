'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from modelClass.customConcept import CustomConcept
from engine.expressionEngine import ExpressionEngine


Initializer()
session = DBConnector().getNewSession()

#customConceptName = 'COST_OF_REVENUE' 
ticker = 'AAPL'
masive = True
copy = False
calculate = False
createRatio = True
deleteCopyCalculate = False
deleteExpression = False
deleteAllFact = False

if (masive):
    if(deleteAllFact):
        CustomFactEngine.deleteCustomFactByStrategy("COPY_CALCULATE", session)
        CustomFactEngine.deleteCustomFactByStrategy("EXPRESSION", session)
    
    if(deleteCopyCalculate):
        CustomFactEngine.deleteCustomFactByCompany(ticker = ticker, fillStrategy = "COPY_CALCULATE", session = session)
    customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "COPY_CALCULATE"), session = session)
    copiedValues = 0
    for customConcept in customConceptList:
        if(copy):
            CustomFactEngine.copyToCustomFact(ticker = ticker, customConcept = customConcept, session = session)
        if(calculate):
            CustomFactEngine.calculateMissingQTDValues(ticker, customConcept, session)
    
    if(deleteExpression):
        CustomFactEngine.deleteCustomFactByCompany(ticker = ticker, fillStrategy = "EXPRESSION", session = session)
    if(createRatio):
        customConceptExpressionList = GenericDao.getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "EXPRESSION"), session = session)
        for customConcept in customConceptExpressionList:
                ExpressionEngine.solveCustomFactFromExpression(ticker, customConcept.conceptName, session)
else:
    customConceptName = "CASH_AND_CASH_EQUIVALENTS"
    if(copy):
        CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
    if(calculate):
        CustomFactEngine.calculateMissingQTDValues(ticker, customConceptName, session)
