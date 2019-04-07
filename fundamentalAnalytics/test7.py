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

customConceptName = 'NET_INCOME_MARGIN' 
customReportName = 'CUSTOM_RATIOS' 
ticker = 'MSFT'
CustomFactEngine.createCustomFactFromExpression(ticker, customConceptName, session, customReportName, 5)
