'''
Created on Mar 17, 2019

@author: afunes
'''

from dao.customFactDao import CustomFactDao
from dao.dao import Dao
from dao.factDao import FactDao
from dao.fileDataDao import FileDataDao
from engine.periodEngine import PeriodEngine
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport


class CustomFactEngine():
    
    def createCustomConcept(self, customConceptName, customReportName, defaultOrder, periodType, fillStrategy, session):
        concept = Dao().getCustomConcept(customConceptName, session)
        if concept is None:
            concept = CustomConcept()
            concept.conceptName = customConceptName
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
            concept.fillStrategy = fillStrategy
            report = Dao().getCustomReport(customReportName, session)
            if report is None:
                report = CustomReport()
                report.shortName = customReportName
            concept.defaultCustomReport = report
        else:
            concept.defaultOrder = defaultOrder
            concept.periodType = periodType
            concept.fillStrategy = fillStrategy
        return concept
    
    def getOrCreateCustomFact(self, customConcept, session=None):
        fact = CustomFactDao().getCustomFact3(customConcept.OID, customConcept.defaultCustomReport.OID, session)
        if fact is None: 
            fact = CustomFact()
            fact.customConceptOID = customConcept.OID
            fact.customReport = customConcept.defaultCustomReport
            #Dao().addObject(objectToAdd=fact, session=session, doFlush=True)
        return fact
    
    def getNewCustomFactValue(self, value, origin, fileDataOID, customConcept, session, endDate=None, periodOID=None):
        customFactValue = CustomFactValue()
        if(periodOID is None):
            customFactValue.period = PeriodEngine().getOrCreatePeriod('QTD', endDate, session)
        else:
            customFactValue.periodOID = periodOID
        customFactValue.value = value
        customFactValue.origin = origin
        customFactValue.fileDataOID = fileDataOID
        customFactValue.customFact = self.getOrCreateCustomFact(customConcept, session)
        return customFactValue
    
    def copyToCustomFact(self, ticker, customConcept, session=None):
        copiedValues = 0
        copiedValues += CustomFactEngine().copyToCustomFactQTDINST(ticker, customConcept, session)
        copiedValues += CustomFactEngine().copyToCustomFactYTD(ticker, customConcept, session)
        if(copiedValues > 0):
            print(customConcept.conceptName + "-> COPY -> COPIED: " + str(copiedValues))
        
    def copyToCustomFactYTD(self, ticker, customConcept, session=None):
        # Solo toma el Q1 en YTD y lo completa en QTD de los custom facts
        newFactValueDict = {}
        customFactList = CustomFactDao().getCustomFactValue3(ticker, customConcept.conceptName, session)
        fileDataCompleted = [x.fileDataOID for x in customFactList]         
        for concept in customConcept.conceptList:
            factValueList = FactDao().getFactValue2(ticker=ticker, periodType='YTD', concept=concept, session=session)
            for row in factValueList:
                if (row.fileDataOID not in fileDataCompleted):  # Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.fileDataOID not in newFactValueDict.keys()):  # Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        if (row.documentFiscalPeriodFocus == 'Q1'):
                            newFactValueDict.setdefault(row.fileDataOID, {})["VALUE"] = row.value
                            newFactValueDict[row.fileDataOID]["END_DATE"] = row.endDate
                    else:
                        print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate) + ' Concept = ' + concept.conceptName)
            
        newCustomFactValueList = []
        for fileDataOID, row in newFactValueDict.items():
            endDate = row["END_DATE"]
            customFactValue = self.getNewCustomFactValue(value=row["VALUE"], origin='COPY', fileDataOID=fileDataOID, customConcept=customConcept, endDate=endDate, session=session)
            newCustomFactValueList.append(customFactValue)
        # print("Copy to YTD->QTD " + customConcept.conceptName + " " + str(len(newCustomFactValueList)))   
        Dao().addObjectList(newCustomFactValueList, session)
        return len(newCustomFactValueList)  
        
    def copyToCustomFactQTDINST(self, ticker, customConcept, session=None):    
        newFactValueDict = {}
        customFactValueList = CustomFactDao().getCustomFactValue3(ticker, customConcept.conceptName, session)
        fileDataCompleted = [cfv.fileDataOID for cfv in customFactValueList]
        for concept in customConcept.conceptList:
            factValueList = FactDao().getFactValue2(ticker=ticker, periodType=customConcept.periodType, concept=concept, session=session)
            for row in factValueList:
                if (row.fileDataOID not in fileDataCompleted):  # Si el periodo de ese customConcept ya no se encuentra resuelto
                    if (row.fileDataOID  not in newFactValueDict.keys()):  # Si el periodo de ese customConcept no se encuentra duplicado desde los conceptos origen
                        newFactValueDict.setdefault(row.fileDataOID, {})["VALUE"] = row.value
                        newFactValueDict[row.fileDataOID]["PERIOD_OID"] = row.periodOID
                    # else:
                        # print('Duplicated period between concepts, periodOID =  ' + str(row.periodOID) + " " + str(row.endDate)+ ' Concept = ' + concept.conceptName)
            
        newCustomFactValueList = []
        for fileDataOID, row in newFactValueDict.items():
                customFactValue = self.getNewCustomFactValue(value=row["VALUE"], origin='COPY', fileDataOID=fileDataOID, customConcept=customConcept, periodOID=row["PERIOD_OID"], session=session)
                newCustomFactValueList.append(customFactValue)
        # print("Copy to " + customConcept.periodType + " " + customConcept.conceptName + " " + str(len(newCustomFactValueList)))   
        Dao().addObjectList(newCustomFactValueList, session)
        return len(newCustomFactValueList)  
    
    def calculateMissingQTDValues(self, ticker, customConcept, session):
        newCustomFactValueList = []
        fileDataToResolve = FileDataDao().getFileDataListWithoutConcept(ticker, customConcept.OID, session)
        if(len(fileDataToResolve) > 0):
            for concept in customConcept.conceptList:
                #    print('CustomConceptToCalculate ' + customConcept.conceptName + " " + concept.conceptName)
                #    print('Period to resolve ' + str(fileDataToResolve.rows))
                    listYTD = FactDao().getFactValue2(ticker, 'YTD', None, concept, session)
                    # print(listYTD)
                    prevRow = None
                    for itemYTD in listYTD:  # itero todos los YTD y cuando corresponde con un faltante busco el YTD anterior y se lo resto o busco los 3 QTD anteriores
                        newCustomFactValue = None
                        sumValue = 0
                        if(itemYTD.fileDataOID in (fd for fd in fileDataToResolve)):
                            if prevRow != None and 80 < (itemYTD.endDate - prevRow.endDate).days < 100 and itemYTD.documentFiscalYearFocus == prevRow.documentFiscalYearFocus:
                                # estrategia de calculo usando el YTD
                                customFactValue = self.getNewCustomFactValue(value=(itemYTD.value - prevRow.value), origin='CALCULATED', fileDataOID=itemYTD.fileDataOID,
                                                                             customConcept=customConcept, session=session, endDate=itemYTD.endDate)
                                newCustomFactValue = customFactValue
                            else:
                                # estrategia de calculo usando los QTD, sumando los ultimos 3 y restandoselo al YTD
                                listQTD = CustomFactDao.getCustomFactValue(ticker, concept.conceptName, 'QTD', session)
                                listQTD_2 = []
                                for itemQTD in listQTD:
                                    if 0 < (itemYTD.endDate - itemQTD.period.endDate).days < 285:
                                        sumValue += itemQTD.value
                                        listQTD_2.append(itemQTD)
                                if(len(listQTD_2) == 3):
                                    customFactValue = self.getNewCustomFactValue(value=(itemYTD.value - sumValue), origin='CALCULATED', fileDataOID=itemYTD.fileDataOID,
                                                                             customConcept=customConcept, session=session, endDate=itemYTD.endDate)
                                    newCustomFactValue = customFactValue
                                # else:
                                    # print('COULDNT CALCULATE PERIOD ' + str(itemYTD.endDate))
                                    # for itemQTD in listQTD_2:
                                    #    print('ITEMS FOUND ' + str(itemQTD.period.endDate))
                            if(newCustomFactValue is not None):
                                # print('NEW CALCULATED FACT VALUES FOR ' + str(customFactValue.period.endDate))
                                newCustomFactValueList.append(newCustomFactValue)
                        else:
                            prevRow = itemYTD
        Dao().addObjectList(newCustomFactValueList, session)
        if(len(fileDataToResolve) > 0):
            print(customConcept.conceptName + " -> CALCULATE - CALCULATED: " + str(len(newCustomFactValueList)) + " PENDING: " + str((len(fileDataToResolve) - len(newCustomFactValueList))))
                
    def deleteCustomFactByCompany(self, ticker, fillStrategy, session):
        customFactValueList = CustomFactDao().getCustomFactValue5(fillStrategy=fillStrategy, ticker=ticker, session=session);
        if(len(customFactValueList) > 0): 
            for itemToDelete in customFactValueList:
                session.delete(itemToDelete)
            session.commit()
            print("DELETED " + str(len(customFactValueList)) + " FOR fillStrategy= " + fillStrategy)

    def deleteCustomFactByStrategy(self, fillStrategy, session):
        customFactValueList = CustomFactDao().getCustomFactValue5(fillStrategy=fillStrategy, session=session);
        if(len(customFactValueList) > 0): 
            for itemToDelete in customFactValueList:
                session.delete(itemToDelete)
            session.commit()
            print("DELETED " + str(len(customFactValueList)) + " FOR fillStrategy= " + fillStrategy)
