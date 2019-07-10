'''
Created on Mar 17, 2019

@author: afunes
'''
from datetime import timedelta
from operator import and_

from sqlalchemy.orm.exc import NoResultFound

from dao.companyDao import CompanyDao
from dao.customFactDao import CustomFactDao
from dao.dao import Dao, GenericDao
from dao.factDao import FactDao
from dao.fileDataDao import FileDataDao
from engine.expressionEngine import ExpressionEngine
from engine.periodEngine import PeriodEngine
from modelClass import period
from modelClass.company import Company
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.period import Period


class CustomFactEngine():
    
    @staticmethod
    def createCustomConcept(customConceptName, customReportName, defaultOrder, periodType, fillStrategy, session):
        concept = Dao.getCustomConcept(customConceptName, session)
        if concept is None:
            concept = CustomConcept()
            concept.conceptName = customConceptName
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
            concept.fillStrategy = fillStrategy
            report = Dao.getCustomReport(customReportName, session)
            if report is None:
                report = CustomReport()
                report.shortName = customReportName
            concept.defaultCustomReport = report
        else:
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
            concept.fillStrategy = fillStrategy
        return concept
    
    def getOrCreateCustomFact(self, ticker, customConceptName, session = None):
        fact = CustomFactDao().getCustomFact3(ticker, customConceptName, session)
        if fact is None: 
            fact = CustomFact()
            customConcept = Dao.getCustomConcept(customConceptName, session)
            fact.customReport = customConcept.defaultCustomReport
            fact.customConcept = customConcept
            fact.company = CompanyDao().getCompany(ticker, session)
        return fact
    
    def copyToCustomFact(self, ticker, customConcept, session = None):
        copiedValues = 0
        copiedValues += CustomFactEngine().copyToCustomFactQTDINST(ticker, customConcept, session)
        copiedValues += CustomFactEngine().copyToCustomFactYTD(ticker, customConcept, session)
        if(copiedValues > 0):
            print(customConcept.conceptName + "-> COPY -> COPIED: " + str(copiedValues))
                
        
    @staticmethod    
    def copyToCustomFactYTD(ticker, customConcept, session = None):
        #Solo toma el Q1 en YTD y lo completa en QTD de los custom facts
        newFactValueDict = {}
        customFactList = CustomFactDao().getCustomFact3(ticker, customConcept.conceptName, session)
        fileDataCompleted = [x.fileDataOID for x in customFactList]         
        for concept in customConcept.conceptList:
            factValueList = FactDao().getFactValue2(ticker = ticker, periodType = 'YTD', concept = concept, session = session)
            for row in factValueList:
                if (row.fileDataOID not in fileDataCompleted):#Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.fileDataOID not in newFactValueDict.keys()):#Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        if (row.documentFiscalPeriodFocus == 'Q1'):
                            newFactValueDict.setdefault(row.fileDataOID, {})["VALUE"] = row.value
                            newFactValueDict[row.fileDataOID]["END_DATE"] = row.endDate
                    else:
                        print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate)+ ' Concept = ' + concept.conceptName)
            
        newCustomFactList = []
        for fileDataOID, row in newFactValueDict.items():
                customFactValue = CustomFactValue()
                endDate = row["END_DATE"]
                period = GenericDao().getOneResult(Period, and_(Period.endDate == endDate, Period.startDate == None), session, raiseNoResultFound = False)
                customFactValue.period = period
                if(period is None):
                    newPeriod = Period()
                    newPeriod.endDate = endDate
                    newPeriod.type = 'QTD'
                    customFactValue.period = newPeriod
                customFactValue.value = row["VALUE"]
                customFactValue.origin = 'COPY'
                customFact = CustomFact()
                customFact.customConceptOID = customConcept.OID
                customFact.customReportOID = customConcept.defaultCustomReportOID
                customFact.fileDataOID = fileDataOID
                customFact.customFactValueList.append(customFactValue)
                newCustomFactList.append(customFact)
        #print("Copy to YTD->QTD " + customConcept.conceptName + " " + str(len(newCustomFactList)))   
        if(len(newCustomFactList) > 0):
            for newCustomFact in newCustomFactList:
                Dao().addObject(objectToAdd = newCustomFact, session = session, doCommit = True) 
        return len(newCustomFactList)  
        
    def copyToCustomFactQTDINST(self, ticker, customConcept, session = None):    
        newFactValueDict = {}
        customFactList = CustomFactDao().getCustomFact3(ticker, customConcept.conceptName, session)
        fileDataCompleted = [x.fileDataOID for x in customFactList]
        for concept in customConcept.conceptList:
            factValueList = FactDao().getFactValue2(ticker = ticker, periodType = customConcept.periodType, concept = concept, session = session)
            for row in factValueList:
                if (row.fileDataOID not in fileDataCompleted):#Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.fileDataOID  not in newFactValueDict.keys()):#Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        newFactValueDict.setdefault(row.fileDataOID, {})["VALUE"] = row.value
                        newFactValueDict[row.fileDataOID]["PERIOD_OID"] = row.periodOID
                    #else:
                        #print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate)+ ' Concept = ' + concept.conceptName)
            
        newCustomFactList = []
        for fileDataOID, row in newFactValueDict.items():
                customFactValue = CustomFactValue()
                customFactValue.periodOID = row["PERIOD_OID"]
                customFactValue.value = row["VALUE"]
                customFactValue.origin = 'COPY'
                customFact = CustomFact()
                customFact.customConceptOID = customConcept.OID
                customFact.customReportOID = customConcept.defaultCustomReportOID
                customFact.fileDataOID = fileDataOID
                customFact.customFactValueList.append(customFactValue)
                newCustomFactList.append(customFact)
        #print("Copy to " + customConcept.periodType + " " + customConcept.conceptName + " " + str(len(newCustomFactList)))   
        if(len(newCustomFactList) > 0):
            for newCustomFact in newCustomFactList:
                Dao().addObject(objectToAdd = newCustomFact, session = session, doCommit = True) 
        return len(newCustomFactList)  
    
    def calculateMissingQTDValues(self, ticker, customConcept, session):
        newCustomFactList = []
        fileDataToResolve = FileDataDao.getFileDataListWithoutConcept(ticker,customConcept.OID, session)
        fd2 = []
        for fd in fileDataToResolve:
            fd2.append(fd.fileDataOID)
        if(len(fd2) > 0):
            for concept in customConcept.conceptList:
                #    print('CustomConceptToCalculate ' + customConcept.conceptName + " " + concept.conceptName)
                #    print('Period to resolve ' + str(fileDataToResolve.rows))
                    listYTD = FactDao().getFactValue2(ticker, 'YTD', None, concept, session)
                    #print(listYTD)
                    prevRow = None
                    for itemYTD in listYTD: # itero todos los YTD y cuando corresponde con un faltante busco el YTD anterior y se lo resto o busco los 3 QTD anteriores
                        newCustomFact = None
                        sumValue = 0
                        if(itemYTD.fileDataOID in (fd for fd in fd2)):
                            if prevRow != None and 80 < (itemYTD.endDate - prevRow.endDate).days < 100 and itemYTD.documentFiscalYearFocus == prevRow.documentFiscalYearFocus:
                                #estrategia de calculo usando el YTD
                                customFactValue = CustomFactValue()
                                customFactValue.value = itemYTD.value - prevRow.value
                                customFactValue.period = PeriodEngine().getOrCreatePeriod(ticker, 'QTD', itemYTD.endDate, session)
                                customFactValue.origin = 'CALCULATED'
                                customFact = CustomFact()
                                customFact.customConceptOID = customConcept.OID
                                customFact.customReportOID = customConcept.defaultCustomReportOID
                                customFact.fileDataOID = itemYTD.fileDataOID
                                customFact.customFactValueList.append(customFactValue)
                                newCustomFact = customFact
                            else:
                                #estrategia de calculo usando los QTD, sumando los ultimos 3 y restandoselo al YTD
                                listQTD = CustomFactDao.getCustomFactValue(ticker, concept.conceptName, 'QTD', session)
                                listQTD_2 = []
                                for itemQTD in listQTD:
                                    if 0 < (itemYTD.endDate - itemQTD.period.endDate).days < 285:
                                        sumValue += itemQTD.value
                                        listQTD_2.append(itemQTD)
                                if(len(listQTD_2) == 3):
                                    customFactValue = CustomFactValue()
                                    customFactValue.value = itemYTD.value - sumValue
                                    customFactValue.period = PeriodEngine().getOrCreatePeriod(ticker, 'QTD', itemYTD.endDate, session)
                                    customFactValue.origin = 'CALCULATED'
                                    customFact = CustomFact()
                                    customFact.customConceptOID = customConcept.OID
                                    customFact.customReportOID = customConcept.defaultCustomReportOID
                                    customFact.fileDataOID = itemYTD.fileDataOID
                                    customFact.customFactValueList.append(customFactValue)
                                    newCustomFact = customFact
                                #else:
                                    #print('COULDNT CALCULATE PERIOD ' + str(itemYTD.endDate))
                                    #for itemQTD in listQTD_2:
                                    #    print('ITEMS FOUND ' + str(itemQTD.period.endDate))
                            if(newCustomFact is not None):
                                #print('NEW CALCULATED FACT VALUES FOR ' + str(customFactValue.period.endDate))
                                newCustomFactList.append(newCustomFact)
                        else:
                            prevRow = itemYTD
        for newCustomFact in newCustomFactList:
            Dao().addObject(objectToAdd = newCustomFact, session = session, doCommit = True)
        if(len(fd2) > 0):
            print(customConcept.conceptName + " -> CALCULATE - CALCULATED: " + str(len(newCustomFactList)) + " PENDING: "  + str((len(fd2) - len(newCustomFactList))))
                
    def deleteCustomFactByCompany(self, ticker, fillStrategy, session):
        try:
            customFactList = CustomFactDao().getCustomFact(fillStrategy = fillStrategy, ticker = ticker, session = session);
        except NoResultFound:
            return None
        for itemToDelete in customFactList:
            if itemToDelete.customConcept.fillStrategy == fillStrategy:
                session.delete(itemToDelete)
        session.commit()

    def deleteCustomFactByStrategy(self, fillStrategy, session):
        try:
            customFactList = CustomFactDao().getCustomFact(fillStrategy = fillStrategy, session = session);
        except NoResultFound:
            return None
        for itemToDelete in customFactList:
            session.delete(itemToDelete)
        session.commit()
