'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from engine.entityFactEngine import EntityFactEngine
from engine.expressionEngine import ExpressionEngine
from modelClass.customConcept import CustomConcept


# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
Initializer()
session = DBConnector().getNewSession()

#customConceptName = 'COST_OF_REVENUE' 
ticker = 'SGC'

deleteCopyCalculate = False
deleteEntityFact = False
deleteExpression = True
deleteAllCustomFact = False


if(deleteAllCustomFact):
    CustomFactEngine().deleteCustomFactByStrategy("COPY_CALCULATE", session)
    CustomFactEngine().deleteCustomFactByStrategy("EXPRESSION", session)

if(deleteCopyCalculate):
    CustomFactEngine().deleteCustomFactByCompany(ticker = ticker, fillStrategy = "COPY_CALCULATE", session = session)

if(deleteExpression):
    CustomFactEngine().deleteCustomFactByCompany(ticker = ticker, fillStrategy = "EXPRESSION", session = session)
    
if(deleteEntityFact):
    EntityFactEngine().deleteCustomFactByCompany(ticker = ticker, session = session)
# if(createRatio):
#     customConceptExpressionList = GenericDao().getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "EXPRESSION"), session = session)
#     for customConcept in customConceptExpressionList:
#             ExpressionEngine().solveCustomFactFromExpression(ticker, customConcept, session)
