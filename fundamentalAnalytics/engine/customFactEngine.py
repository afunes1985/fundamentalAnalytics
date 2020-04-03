'''
Created on Mar 17, 2019

@author: afunes
'''

from dao.customFactDao import CustomFactDao
from dao.dao import Dao
from dao.factDao import FactDao
from engine.periodEngine import PeriodEngine
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from valueobject.valueobject import CustomFactValueVO
from valueobject.constant import Constant


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
            # Dao().addObject(objectToAdd=fact, session=session, doFlush=True)
        return fact
    
    def getNewCustomFactValue(self, value, origin, fileDataOID, customConcept, session, endDate=None, periodOID=None):
        customFactValue = CustomFactValue()
        if(periodOID is None and endDate is not None):
            customFactValue.period = PeriodEngine().getOrCreatePeriod('QTD', endDate, session)
        else:
            customFactValue.periodOID = periodOID
        customFactValue.value = value
        customFactValue.origin = origin
        customFactValue.fileDataOID = fileDataOID
        customFactValue.customFact = self.getOrCreateCustomFact(customConcept, session)
        return customFactValue
                
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

    def copyToCustomFact2(self, fileData, customConceptList, session):
        newCFVDict = {}
        self.copyToCustomFactQTDINST2(fileData, customConceptList, newCFVDict, session)
        self.copyToCustomFactYTD2(fileData, customConceptList, newCFVDict, session)
        return newCFVDict.values()
            
    def copyToCustomFactQTDINST2(self, fileData, customConceptList, newCFVDict, session):  
        factValueRS = FactDao().getFactValue3(periodTypeList = ['QTD', 'INST'], fileDataOID=fileData.OID, session=session)
        rsDict = {}
        for row in factValueRS:
            rsDict.setdefault(str(row.conceptOID)+"-"+row.type, []).append(row)  
        for customConcept in customConceptList:
            for relationConcept in customConcept.relationConceptList:
                for row in rsDict.get(str(relationConcept.conceptOID)+"-"+customConcept.periodType, []):
                    if(customConcept.conceptName not in newCFVDict.keys()):
                        cfvVO = CustomFactValueVO(value=row.value, origin='COPY',fileDataOID=fileData.OID, 
                                          customConcept=customConcept, periodOID=row.periodOID, order_ = relationConcept.order_ )
                        newCFVDict[customConcept.conceptName] = cfvVO
    
    def copyToCustomFactYTD2(self, fileData, customConceptList, newCFVDict, session):
        # Solo toma el Q1 en YTD y lo completa en QTD de los custom facts
        if (fileData.documentFiscalPeriodFocus == 'Q1'):
            factValueRS = FactDao().getFactValue3(periodTypeList=['YTD'], fileDataOID=fileData.OID, session=session)
            rsDict = {}
            for row in factValueRS:
                rsDict.setdefault(row.conceptOID, []).append(row)

            for customConcept in customConceptList:
                for relationConcept in customConcept.relationConceptList:
                    for row in rsDict.get(relationConcept.conceptOID, []):
                        if(customConcept.conceptName not in newCFVDict.keys()):
                            cfvVO = CustomFactValueVO(value=row.value, origin='COPY',fileDataOID=fileData.OID, 
                                          customConcept=customConcept, endDate=row.endDate, order_ = relationConcept.order_ )
                            newCFVDict[customConcept.conceptName] = cfvVO
    
    def getPrevPeriodFocus(self, currentPeriodFocus):
        prevPeriodFocus = Constant.PERIOD_FOCUS_REL_DICT.get(currentPeriodFocus, None)
        if(prevPeriodFocus is None):
            raise Exception("Period Focus doesn't found in constant" + currentPeriodFocus)
        return prevPeriodFocus
        
    def calculateMissingQTDValues2(self, fileData, customConceptList, session):
        cfvVOList = []
        ytdDict = {}
        qtdDict = {}
        if(fileData.documentFiscalYearFocus is None):
            raise Exception("fileData documentFiscalYearFocus is None")
        currentPeriodFocus = fileData.documentFiscalPeriodFocus
        listYTD = FactDao().getFactValue4(companyOID=fileData.companyOID, periodType='YTD', documentFiscalYearFocus=fileData.documentFiscalYearFocus, session=session)
        for itemYTD in listYTD:
            itemYTDDict = ytdDict.get(itemYTD.conceptOID, {})
            itemYTDDict[itemYTD.documentFiscalPeriodFocus] = itemYTD
            ytdDict[itemYTD.conceptOID] = itemYTDDict
        
        listQTD = FactDao().getFactValue4(companyOID=fileData.companyOID, periodType='QTD', documentFiscalYearFocus=fileData.documentFiscalYearFocus, session=session)
        for itemQTD in listQTD:
            itemQTDDict = qtdDict.get(itemQTD.conceptOID, {})
            itemQTDDict[itemQTD.documentFiscalPeriodFocus] = itemQTD
            qtdDict[itemQTD.conceptOID] = itemQTDDict
        
        for customConcept in customConceptList:
            for relationConcept in customConcept.relationConceptList:
                customFactValueVO = None
                #YTD STRATEGY Q1
                if(currentPeriodFocus == 'Q1'):
                    itemYTDDict = ytdDict.get(relationConcept.concept.OID, None)
                    if(itemYTDDict is not None):
                        itemYTD = itemYTDDict.get('Q1', None)
                        if(itemYTD is not None):
                            print("NEW FACT VALUE 1 " + customConcept.conceptName + " " + str(itemYTD.value) + " " + relationConcept.concept.conceptName)
                            customFactValueVO = CustomFactValueVO(value=(itemYTD.value), origin='CALCULATED', 
                                                                  fileDataOID=fileData.OID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
                #YTD STRATEGY YTD-YTD
                itemYTDDict = ytdDict.get(relationConcept.concept.OID, None)
                if(itemYTDDict is not None):
                    itemYTD = itemYTDDict.get(currentPeriodFocus, None)
                    prevYTD = itemYTDDict.get(self.getPrevPeriodFocus(currentPeriodFocus), None)
                    if(itemYTD is not None and prevYTD is not None):
                        print("NEW FACT VALUE 2 " + customConcept.conceptName + " " + str(itemYTD.value - prevYTD.value) + " " + relationConcept.concept.conceptName)
                        customFactValueVO = CustomFactValueVO(value=(itemYTD.value - prevYTD.value), origin='CALCULATED', 
                                                                  fileDataOID=fileData.OID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
                
                #YTD STRATEFY YTD-QTD
                if(itemYTDDict is not None):
                    itemYTD = itemYTDDict.get(currentPeriodFocus, None)
                    itemQTDDict = qtdDict.get(relationConcept.concept.OID, None)
                    if(itemQTDDict is not None):
                        q1QTD = itemQTDDict.get('Q1', None)
                        q2QTD = itemQTDDict.get('Q2', None)
                        q3QTD = itemQTDDict.get('Q3', None)
                        q4QTD = itemQTDDict.get('Q4', None)
                        if q4QTD is None:
                            q4QTD = itemQTDDict.get('FY', None)
                        if(itemYTD is not None and q1QTD is not None and currentPeriodFocus == 'Q2'):
                            print("NEW FACT VALUE 3 " + customConcept.conceptName + " " + str(itemYTD.value - q1QTD.value) + " " + relationConcept.concept.conceptName)
                            customFactValueVO = CustomFactValueVO(value=(itemYTD.value - q1QTD.value), origin='CALCULATED', 
                                                                      fileDataOID=fileData.OID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
                        elif(itemYTD is not None and q1QTD is not None and q2QTD is not None and currentPeriodFocus == 'Q3'):
                            print("NEW FACT VALUE 4 " + customConcept.conceptName + " " + str(itemYTD.value - q1QTD.value - q2QTD.value) + " " + relationConcept.concept.conceptName)
                            customFactValueVO = CustomFactValueVO(value=(itemYTD.value - q1QTD.value - q2QTD.value), origin='CALCULATED', 
                                                                      fileDataOID=fileData.OID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
                        elif(itemYTD is not None and q1QTD is not None and q2QTD is not None and q3QTD is not None and (currentPeriodFocus == 'Q4' or currentPeriodFocus == 'FY')):
                            print("NEW FACT VALUE 5 " + customConcept.conceptName + " " + str(itemYTD.value - q1QTD.value - q2QTD.value - q3QTD.value) + " " + relationConcept.concept.conceptName)
                            customFactValueVO = CustomFactValueVO(value=(itemYTD.value - q1QTD.value - q2QTD.value), origin='CALCULATED', 
                                                                      fileDataOID=fileData.OID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
                            
                            
                if(customFactValueVO is not None):
                    cfvVOList.append(customFactValueVO)
                    break                
#                 prevRow = None 
#                 customFactValueVO = None
#                 for itemYTD in ytdDict.get(relationConcept.concept.OID, []):  # itero todos los YTD y cuando corresponde con un faltante busco el YTD anterior y se lo resto o busco los 3 QTD anteriores
#                     if prevRow != None and 80 < (itemYTD.endDate - prevRow.endDate).days < 100 and itemYTD.fileDataOID == fileData.OID:
#                         # estrategia de calculo usando el YTD
#                         print("NEW FACT VALUE 1 " + customConcept.conceptName + " " + str(itemYTD.value - prevRow.value) + " " + relationConcept.concept.conceptName)
#                         customFactValueVO = CustomFactValueVO(value=(itemYTD.value - prevRow.value), origin='CALCULATED', 
#                                                               fileDataOID=itemYTD.fileDataOID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
#                         break
#                     else:
#                         prevRow = itemYTD
#                         
#                     if customFactValueVO is None:
#                         listQTD = CustomFactDao().getCustomFactValue4(companyOID=fileData.company.OID, documentFiscalYearFocus=fileData.documentFiscalYearFocus, customConceptOID=customConcept.OID, session=session)
#                         sumValue = 0
#                         listQTD_2 = []
#                         if itemYTD.documentFiscalPeriodFocus == 'Q2':
#                             for itemQTD in listQTD:
#                                 if 0 < (itemYTD.endDate - itemQTD.period.endDate).days < 285 and itemQTD.fileData.documentFiscalPeriodFocus == 'Q1':
#                                     sumValue += itemQTD.value
#                                     listQTD_2.append(itemQTD)
#                                     break
#                             if(len(listQTD_2) == 1 and itemYTD.fileDataOID == fileData.OID):
#                                 print("NEW FACT VALUE 2 " + customConcept.conceptName + " " + str(itemYTD.value - sumValue))
#                                 customFactValueVO = CustomFactValueVO(value=(itemYTD.value - sumValue), origin='CALCULATED', 
#                                                                   fileDataOID=itemYTD.fileDataOID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
#                                 break
#                         else:
#                             # estrategia de calculo usando los QTD, sumando los ultimos 3 y restandoselo al YTD
#                             for itemQTD in listQTD:
#                                 if 0 < (itemYTD.endDate - itemQTD.period.endDate).days < 285:
#                                     sumValue += itemQTD.value
#                                     listQTD_2.append(itemQTD)
#                             if(len(listQTD_2) == 3 and itemYTD.fileDataOID == fileData.OID):
#                                 print("NEW FACT VALUE 3 " + customConcept.conceptName + " " + str(itemYTD.value - sumValue))
#                                 customFactValueVO = CustomFactValueVO(value=(itemYTD.value - sumValue), origin='CALCULATED', 
#                                                                   fileDataOID=itemYTD.fileDataOID, customConcept=customConcept, endDate=itemYTD.endDate, order_ = customConcept.defaultOrder)
#                                 break
                
        return cfvVOList
