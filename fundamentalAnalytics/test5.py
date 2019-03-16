'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from engine.calculator import Calculator
from modelClass.customReport import CustomReport
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from dao.dao import Dao

Initializer()
session = DBConnector().getNewSession()

concept = Dao.getCustomConcept('CUSTOM_GROSS_PROFIT', session)
if concept is None:
    concept = CustomConcept()
    concept.conceptName = 'CUSTOM_GROSS_PROFIT'
    concept.defaultOrder = 1
    report = Dao.getCustomReport('CUSTOM_RATIOS', session)
    if report is None:
        report = CustomReport()
        report.shortName = 'CUSTOM_RATIOS'
    concept.defaultCustomReport = report

company = Dao.getCompany('MSFT', session)
if (company is None):
    raise Exception('FAIL')

fact = Dao.getCustomFact(company, concept, concept.defaultCustomReport, session)
if fact is None: 
    fact = CustomFact()
    fact.customReport = concept.defaultCustomReport
    fact.customConcept = concept
    fact.company = company

factValueList = Calculator.solveRule(company.ticker, None);
fact.customFactValueList = factValueList

Dao.addObject(objectToAdd = fact, session = session, doCommit = True)



