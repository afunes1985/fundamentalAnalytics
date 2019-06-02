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
from engine.expressionEngine import ExpressionEngine


Initializer()
session = DBConnector().getNewSession()

#customConceptName = 'COST_OF_REVENUE' 
ticker = 'AAPL'
masive = True
copy = False
calculate = False
deleteCopyCalculate = False
deleteStrategy = False
createRatio = True
if (masive):
    if(deleteCopyCalculate):
        CustomFactEngine.deleteCustomFactByCompany(ticker = ticker, fillStrategy = "COPY_CALCULATE", session = session)
    customConceptList = GenericDao.getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "COPY_CALCULATE"), session = session)
    valuedCopied = 0
    for customConcept in customConceptList:
        if(copy):
            valuedCopied += CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConcept.conceptName, session = session)
        if(calculate):
            CustomFactEngine.completeMissingQTDValues(ticker, customConcept.conceptName, session)
    
    if(deleteStrategy):
        CustomFactEngine.deleteCustomFactByCompany(ticker = ticker, fillStrategy = "EXPRESSION", session = session)
    if(createRatio):
        customConceptExpressionList = GenericDao.getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "EXPRESSION"), session = session)
        for customConcept in customConceptExpressionList:
                ExpressionEngine.solveCustomFactFromExpression(ticker, customConcept.conceptName, session)
else:
    customConceptName = "LONG_TERM_INVESTMENTS"
    if(copy):
        CustomFactEngine.copyToCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
    if(calculate):
        CustomFactEngine.completeMissingQTDValues(ticker, customConceptName, session)
