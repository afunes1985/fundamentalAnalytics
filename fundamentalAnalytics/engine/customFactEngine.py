'''
Created on Mar 17, 2019

@author: afunes
'''
from datetime import timedelta
from operator import and_

from dao.dao import Dao, GenericDao
from engine.expressionEngine import ExpressionEngine
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.period import Period


class CustomFactEngine():
    
    @staticmethod
    def createCustomConcept(customConceptName, customReportName, defaultOrder, session):
        concept = Dao.getCustomConcept(customConceptName, session)
        if concept is None:
            concept = CustomConcept()
            concept.conceptName = customConceptName
            concept.defaultOrder = defaultOrder
            report = Dao.getCustomReport(customReportName, session)
            if report is None:
                report = CustomReport()
                report.shortName = customReportName
            concept.defaultCustomReport = report
        return concept
    
    @staticmethod
    def createCustomFact(ticker, customConceptName, customReportName, defaultOrder, session):
        customConcept = CustomFactEngine.createCustomConcept(customConceptName, customReportName, defaultOrder, session)
        company = Dao.getCompany(ticker, session)
        if (company is None):
            raise Exception('Company not found ' + ticker )
        
        fact = Dao.getCustomFact(company, customConcept, customConcept.defaultCustomReport, session)
        if fact is None: 
            fact = CustomFact()
            fact.customReport = customConcept.defaultCustomReport
            fact.customConcept = customConcept
            fact.company = company
        return fact
    
    @staticmethod
    def createCustomFactValueFromExpression(fact, customConceptName, session):
        fact.customFactValueList = ExpressionEngine.solveExpression(fact.company.ticker, customConceptName);
        Dao.addObject(objectToAdd = fact, session = session, doCommit = True)
        
    @staticmethod    
    def copyToCustomFact(ticker, customConceptName, customReportName, defaultOrder, session):
        customFact = CustomFactEngine.createCustomFact(ticker, customConceptName, customReportName, defaultOrder, session)
        factValueList = Dao.getFactValue2(ticker = ticker, periodType = "QTD", conceptList = customFact.customConcept.conceptList ,session = session)
        for row in factValueList:
            customFactValue = CustomFactValue()
            customFactValue.periodOID = row.periodOID
            customFactValue.value = row.value
            customFactValue.origin = 'COPY'
            customFact.customFactValueList.append(customFactValue)
        print("Copy to " + customConceptName + " " + str(len(factValueList)))    
        Dao.addObject(objectToAdd = customFact, session = session, doCommit = True)    
    
    @staticmethod       
    def completeMissingQTDValues(ticker, conceptName, customConceptName, session):
        listQTD = Dao.getFactValue2(ticker, conceptName, 'QTD', '10-Q', session)
        listYTD = Dao.getFactValue2(ticker, conceptName, 'YTD', '10-K', session)
        listCustomQTD = Dao.getCustomFactValue(ticker, customConceptName, 'QTD', session) 
        periodList =  Dao.getPeriodByFact(ticker, conceptName, 'QTD', session)
        newQTDList = []
        for itemYTD in listYTD:
            print("YTD " + str(itemYTD.period.endDate) + " " + str(itemYTD.value))
            listQTD_2 = []
            continue_ = False 
            for customQTD in listCustomQTD:
                if itemYTD.period.endDate == customQTD.period.endDate:
                    print ("BREAK")
                    continue_ = True
                    break
            if continue_:
                continue
            
            for itemQTD in listQTD:
                if 0 < (itemYTD.period.endDate - itemQTD.period.endDate).days < 380:
                    listQTD_2.append(itemQTD)
            #listQTD.remove(listQTD_2)
            if(len(listQTD_2) == 3):
                sumValue = 0
                for itemQTD_2 in listQTD_2:
                    sumValue += itemQTD_2.value
                    print(sumValue)
                customFactValue = CustomFactValue()
                customFactValue.value = itemYTD.value - sumValue
                customFactValue.origin = 'CALCULATED'
                for period in periodList:
                    if period.endDate == itemYTD.period.endDate:
                        customFactValue.period = period
                newQTDList.append(customFactValue)
            else:
                #COMPLETAR CON T-1 de YTD, t-(t-1) 
                print("Error")
        
        for it in newQTDList:
            print(it.period.endDate)
        
        company = Dao.getCompany(ticker, session)
        if (company is None):
            raise Exception('Company not found ' + ticker )
        customConcept = Dao.getCustomConcept(customConceptName, session)
        customFact = Dao.getCustomFact(company, customConcept, customConcept.defaultCustomReport, session)
        customFact.customFactValueList.extend(newQTDList)
        Dao.addObject(objectToAdd = customFact, session = session, doCommit = True)        
            