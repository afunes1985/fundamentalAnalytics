'''
Created on Mar 17, 2019

@author: afunes
'''
from datetime import timedelta
from operator import and_

from sqlalchemy.orm.exc import NoResultFound

from dao.customFactDao import CustomFactDao
from dao.dao import Dao, GenericDao
from dao.factDao import FactDao
from engine.expressionEngine import ExpressionEngine
from modelClass import period
from modelClass.company import Company
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.period import Period


class CustomFactEngine():
    
    @staticmethod
    def createCustomConcept(customConceptName, customReportName, defaultOrder, periodType, session):
        concept = Dao.getCustomConcept(customConceptName, session)
        if concept is None:
            concept = CustomConcept()
            concept.conceptName = customConceptName
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
            report = Dao.getCustomReport(customReportName, session)
            if report is None:
                report = CustomReport()
                report.shortName = customReportName
            concept.defaultCustomReport = report
        else:
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
        return concept
    
    @staticmethod
    def createCustomFact(ticker, customConceptName, session = None):
        customConcept = Dao.getCustomConcept(customConceptName, session)
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
    def copyToCustomFact(ticker, customConceptName, session = None):
        copiedValues = 0
        customFact = CustomFactEngine.createCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
        copiedValues += CustomFactEngine.copyToCustomFactQTD(customFact, ticker, customConceptName, session)
        copiedValues += CustomFactEngine.copyToCustomFactYTD(customFact, ticker, customConceptName, session)
        return copiedValues
        
    @staticmethod    
    def copyToCustomFactYTD(customFact, ticker, customConceptName, session = None):
        newFactValueDict = {}
        periodsCompleted = [x.period.getKeyDate() for x in customFact.customFactValueList]            
        for concept in customFact.customConcept.conceptList:
            factValueList = FactDao.getFactValue2(ticker = ticker, periodType = 'YTD', concept = concept, session = session)
            for row in factValueList:
                if (row.endDate not in periodsCompleted):#Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.endDate not in newFactValueDict.keys()):#Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        if (row.documentFiscalPeriodFocus == 'Q1'):
                            newFactValueDict[row.endDate] = row.value
                    else:
                        print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate)+ ' Concept = ' + concept.conceptName)
            
        newFactValues = []
        for endDate, value in newFactValueDict.items():
                customFactValue = CustomFactValue()
                period = GenericDao.getOneResult(Period, and_(Period.endDate == endDate, Period.startDate == None), session, raiseError = False)
                customFactValue.period = period
                if(period is None):
                    newPeriod = Period()
                    newPeriod.endDate = endDate
                    newPeriod.type = 'QTD'
                    customFactValue.period = newPeriod
                customFactValue.value = value
                customFactValue.origin = 'COPY'
                newFactValues.append(customFactValue)
        print("Copy to YTD->QTD " + customConceptName + " " + str(len(newFactValues)))   
        if(len(newFactValues) > 0):
            customFact.customFactValueList.extend(newFactValues)
            Dao.addObject(objectToAdd = customFact, session = session, doCommit = True) 
        return len(newFactValues)  
        
    @staticmethod    
    def copyToCustomFactQTD(customFact, ticker, customConceptName, session = None):    
        newFactValueDict = {}
        periodsCompleted = [x.periodOID for x in customFact.customFactValueList]
        for concept in customFact.customConcept.conceptList:
            factValueList = FactDao.getFactValue2(ticker = ticker, periodType = customFact.customConcept.periodType, concept = concept, session = session)
            for row in factValueList:
                if (row.periodOID not in periodsCompleted):#Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.periodOID not in newFactValueDict.keys()):#Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        newFactValueDict[row.periodOID] = row.value
                    else:
                        print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate)+ ' Concept = ' + concept.conceptName)
            
        newFactValues = []
        for periodOID, value in newFactValueDict.items():
                customFactValue = CustomFactValue()
                customFactValue.periodOID = periodOID
                customFactValue.value = value
                customFactValue.origin = 'COPY'
                newFactValues.append(customFactValue)
        print("Copy to QTD " + customConceptName + " " + str(len(newFactValues)))   
        if(len(newFactValues) > 0):
            customFact.customFactValueList.extend(newFactValues)
            Dao.addObject(objectToAdd = customFact, session = session, doCommit = True) 
        return len(newFactValues)  
    
    @staticmethod       
    def calculateMissingQTDValues(ticker, customConceptName, session):
        customFact = CustomFactDao.getCustomFact2(ticker, customConceptName, session)
        if(customFact is not None):
            for concept in customFact.customConcept.conceptList:
                periodFactYTDList = Dao.getPeriodByFact(ticker, concept.conceptName, 'YTD', session)
                periodDefaultQTDList = Dao.getPeriodByFact2(ticker, 'QTD', session)
                periodCustomFactQTDList = Dao.getPeriodByCustomFact(ticker, customConceptName, 'QTD', session)
                periodToResolve = [x for x in periodFactYTDList if x[1] not in (y[1] for y in periodCustomFactQTDList)]
                if(len(periodToResolve) > 0):
                    print('CustomConceptToCalculate ' + customConceptName + " " + concept.conceptName)
                    print('Period to resolve ' + str(periodToResolve))
                    listYTD = FactDao.getFactValue2(ticker, 'YTD', None, concept, session)
                    #print(listYTD)
                    prevRow = None
                    for itemYTD in listYTD: # itero todos los YTD y cuando corresponde con un faltante busco el YTD anterior y se lo resto o busco los 3 QTD anteriores
                        newFactValue = None
                        sumValue = 0
                        if(itemYTD.endDate in (itemYTD[1] for itemYTD in periodToResolve)):
                            if prevRow != None and 80 < (itemYTD.endDate - prevRow.endDate).days < 100 and itemYTD.documentFiscalYearFocus == prevRow.documentFiscalYearFocus:
                                #estrategia de calculo usando el YTD
                                periodOID = None
                                for defaultPeriodRow in periodDefaultQTDList:
                                    if(defaultPeriodRow.endDate == itemYTD.endDate):
                                        periodOID = defaultPeriodRow[0]
                                        break
                                customFactValue = CustomFactValue()
                                customFactValue.value = itemYTD.value - prevRow.value
                                if(periodOID is None):
                                    period = GenericDao.getOneResult(Period, and_(Period.endDate == itemYTD.endDate, Period.startDate == None), session, raiseError = False)
                                    customFactValue.period = period
                                    if(period is None):
                                        newPeriod = Period()
                                        newPeriod.endDate = itemYTD.endDate
                                        newPeriod.type = 'QTD'
                                        customFactValue.period = newPeriod
                                else:
                                    customFactValue.periodOID = periodOID
                                    customFactValue.period = GenericDao.getOneResult(Period, (Period.OID == periodOID), session)
                                customFactValue.origin = 'CALCULATED'
                                newFactValue = customFactValue
                            else:
                                #estrategia de calculo usando los QTD, sumando los ultimos 3 y restandoselo al YTD
                                listQTD = CustomFactDao.getCustomFactValue(ticker, customConceptName, 'QTD', session)
                                listQTD_2 = []
                                periodOID = None
                                for itemQTD in listQTD:
                                    if 0 < (itemYTD.endDate - itemQTD.period.endDate).days < 285:
                                        sumValue += itemQTD.value
                                        listQTD_2.append(itemQTD)
                                if(len(listQTD_2) == 3):
                                    for defaultPeriodRow in periodDefaultQTDList:
                                        if(defaultPeriodRow.endDate == itemYTD.endDate):
                                            periodOID = defaultPeriodRow[0]
                                            break
                                    customFactValue = CustomFactValue()
                                    customFactValue.value = itemYTD.value - sumValue
                                    if(periodOID is None):
                                        newPeriod = Period()
                                        newPeriod.endDate = itemYTD.endDate
                                        newPeriod.type = 'QTD'
                                        customFactValue.period = newPeriod
                                    else:
                                        customFactValue.periodOID = periodOID
                                        customFactValue.period = GenericDao.getOneResult(Period, Period.OID == periodOID, session)
                                    customFactValue.origin = 'CALCULATED'
                                    newFactValue = customFactValue
                                else:
                                    print('COULDNT CALCULATE PERIOD ' + str(itemYTD.endDate))
                                    for itemQTD in listQTD_2:
                                        print('ITEMS FOUND ' + str(itemQTD.period.endDate))
                            
                            if(newFactValue is not None):
                                print('NEW CALCULATED FACT VALUES FOR ' + str(newFactValue.period.endDate))
                                customFact.customFactValueList.append(newFactValue)
                                Dao.addObject(objectToAdd = customFact, session = session, doCommit = True)
                        else:
                            prevRow = itemYTD
                
    @staticmethod       
    def deleteCustomFactByCompany(ticker, fillStrategy, session):
        try:
            company = GenericDao.getOneResult(objectClazz = Company, condition = (Company.ticker == ticker),session = session)
        except NoResultFound:
            return None
        for itemToDelete in company.customFactList:
            if itemToDelete.customConcept.fillStrategy == fillStrategy:
                session.delete(itemToDelete)
        session.commit()
