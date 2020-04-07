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
        factValueRS = FactDao().getFactValue3(periodTypeList=['QTD', 'INST'], fileDataOID=fileData.OID, session=session)
        rsDict = {}
        for row in factValueRS:
            rsDict.setdefault(str(row.conceptOID) + "-" + row.type, []).append(row)  
        for customConcept in customConceptList:
            for relationConcept in customConcept.relationConceptList:
                for row in rsDict.get(str(relationConcept.conceptOID) + "-" + customConcept.periodType, []):
                    if(customConcept.conceptName not in newCFVDict.keys()):
                        cfvVO = CustomFactValueVO(value=row.value, origin='COPY', fileDataOID=fileData.OID,
                                          customConcept=customConcept, periodOID=row.periodOID, order_=relationConcept.order_)
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
                            cfvVO = CustomFactValueVO(value=row.value, origin='COPY', fileDataOID=fileData.OID,
                                          customConcept=customConcept, endDate=row.endDate, order_=relationConcept.order_)
                            newCFVDict[customConcept.conceptName] = cfvVO
    
    def getPrevPeriodFocus(self, currentPeriodFocus):
        prevPeriodFocus = Constant.PERIOD_FOCUS_REL_DICT.get(currentPeriodFocus, None)
        if(prevPeriodFocus is None):
            raise Exception("Period Focus doesn't found in constant" + currentPeriodFocus)
        return prevPeriodFocus
        
    def calculateMissingQTDValues2(self, fileData, customConceptList, session):
        cfvVOList = []
        mainYTDDict = {}
        mainQTDDict = {}
        mainCustomQTDDict = {}
        if(fileData.documentFiscalYearFocus is None):
            raise Exception("fileData documentFiscalYearFocus is None")
        currentPeriodFocus = fileData.documentFiscalPeriodFocus
        listYTD = FactDao().getFactValue4(companyOID=fileData.companyOID, periodType='YTD', documentFiscalYearFocus=fileData.documentFiscalYearFocus, session=session)
        for currentYTD in listYTD:
            itemYTDDict = mainYTDDict.get(currentYTD.conceptOID, {})
            itemYTDDict[currentYTD.documentFiscalPeriodFocus] = currentYTD
            mainYTDDict[currentYTD.conceptOID] = itemYTDDict
        
        listQTD = FactDao().getFactValue4(companyOID=fileData.companyOID, periodType='QTD', documentFiscalYearFocus=fileData.documentFiscalYearFocus, session=session)
        for itemQTD in listQTD:
            itemQTDDict = mainQTDDict.get(itemQTD.conceptOID, {})
            itemQTDDict[itemQTD.documentFiscalPeriodFocus] = itemQTD
            mainQTDDict[itemQTD.conceptOID] = itemQTDDict
        
        listCustomQTD = CustomFactDao().getCustomFactValue4(companyOID=fileData.companyOID, periodType='QTD', documentFiscalYearFocus=fileData.documentFiscalYearFocus, session=session)
        for itemCustomQTD in listCustomQTD:
            itemQTDDict = mainCustomQTDDict.get(itemCustomQTD.customConceptOID, {})
            itemQTDDict[itemCustomQTD.documentFiscalPeriodFocus] = itemCustomQTD
            mainCustomQTDDict[itemCustomQTD.customConceptOID] = itemQTDDict
            
        for customConcept in customConceptList:
            currentYTD = None
            customFactValueVO = None
            itemCustomQTDDict = mainCustomQTDDict.get(customConcept.OID, None)
            q1QTD = None
            q2QTD = None
            q3QTD = None
            q4QTD = None
            prevYTD = None
            itemYTDDict = None
            itemQTDDict = None
            q1QTD = self.getQTDItem(q1QTD, itemCustomQTDDict, 'Q1')
            q2QTD = self.getQTDItem(q2QTD, itemCustomQTDDict, 'Q2')
            q3QTD = self.getQTDItem(q3QTD, itemCustomQTDDict, 'Q3')
            q4QTD = self.getQTDItem(q4QTD, itemCustomQTDDict, 'Q4')
            if q4QTD is None:
                        q4QTD = self.getQTDItem(q4QTD, itemCustomQTDDict, 'FY')
            for relationConcept in customConcept.relationConceptList:
                itemYTDDict = mainYTDDict.get(relationConcept.concept.OID, None)
                if(itemYTDDict is not None):
                    # FILL DATA -> currentYTD
                    if(currentYTD is None):
                        currentYTD = itemYTDDict.get(currentPeriodFocus, None)
                    # FILL DATA -> prevYTD
                    if(prevYTD is None):
                        prevYTD = itemYTDDict.get(self.getPrevPeriodFocus(currentPeriodFocus), None)
                # FILL DATA -> q1QTD, q2QTD, q3QTD, q4QTD                   TODO set duplicated exception
                itemQTDDict = mainQTDDict.get(relationConcept.concept.OID, None)
                if(itemQTDDict is not None):
                    q1QTD = self.getQTDItem(q1QTD, itemQTDDict, 'Q1')
                    q2QTD = self.getQTDItem(q2QTD, itemQTDDict, 'Q2')
                    q3QTD = self.getQTDItem(q3QTD, itemQTDDict, 'Q3')
                    q4QTD = self.getQTDItem(q4QTD, itemQTDDict, 'Q4')
                    if q4QTD is None:
                        q4QTD = self.getQTDItem(q4QTD, itemQTDDict, 'FY')
                # YTD STRATEGY Q1
                if(currentPeriodFocus == 'Q1'):
                    if(itemYTDDict is not None):
                        itemYTDq1 = itemYTDDict.get('Q1', None)
                        if(itemYTDq1 is not None):
                            print("NEW FACT VALUE 1 " + customConcept.conceptName + " " + str(itemYTDq1.value) + " " + relationConcept.concept.conceptName)
                            customFactValueVO = self.fillCustomFactValueVO(value=itemYTDq1.value, fileData=fileData, customConcept=customConcept, endDate=itemYTDq1.endDate) 
                            break  # deja de recorrer los conceptos 
            if customFactValueVO is None:
                # YTD STRATEGY YTD-YTD
                if(currentYTD is not None and prevYTD is not None):
                    print("NEW FACT VALUE 2 " + customConcept.conceptName + " " + str(currentYTD.value - prevYTD.value) + " " + relationConcept.concept.conceptName)
                    customFactValueVO = self.fillCustomFactValueVO(value=(currentYTD.value - prevYTD.value), fileData=fileData, customConcept=customConcept, endDate=currentYTD.endDate) 
                # YTD STRATEGY YTD-QTD
                if(currentYTD is not None and customFactValueVO is None):
                    if(q1QTD is not None and currentPeriodFocus == 'Q2'):
                        print("NEW FACT VALUE 3 " + customConcept.conceptName + " " + str(currentYTD.value - q1QTD.value) + " " + relationConcept.concept.conceptName)
                        customFactValueVO = self.fillCustomFactValueVO(value=(currentYTD.value - q1QTD.value), fileData=fileData, customConcept=customConcept, endDate=currentYTD.endDate)
                    elif(q1QTD is not None and q2QTD is not None and currentPeriodFocus == 'Q3'):
                        print("NEW FACT VALUE 4 " + customConcept.conceptName + " " + str(currentYTD.value - q1QTD.value - q2QTD.value) + " " + relationConcept.concept.conceptName)
                        customFactValueVO = self.fillCustomFactValueVO(value=(currentYTD.value - q1QTD.value - q2QTD.value), fileData=fileData, customConcept=customConcept, endDate=currentYTD.endDate)
                    elif(q1QTD is not None and q2QTD is not None and q3QTD is not None and (currentPeriodFocus == 'Q4' or currentPeriodFocus == 'FY')):
                        print("NEW FACT VALUE 5 " + customConcept.conceptName + " " + str(currentYTD.value - q1QTD.value - q2QTD.value - q3QTD.value) + " " + relationConcept.concept.conceptName)
                        customFactValueVO = self.fillCustomFactValueVO(value=(currentYTD.value - q1QTD.value - q2QTD.value - q3QTD.value), fileData=fileData, customConcept=customConcept, endDate=currentYTD.endDate)
                        
            if(customFactValueVO is not None):
                cfvVOList.append(customFactValueVO)
                
        return cfvVOList
    
    def fillCustomFactValueVO(self, value, fileData, customConcept, endDate):
        customFactValueVO = CustomFactValueVO(value=value, origin='CALCULATED',
                                fileDataOID=fileData.OID, customConcept=customConcept, endDate=endDate, order_=customConcept.defaultOrder)
        return customFactValueVO

    def getQTDItem(self, oldQTDItem, itemQTDDict, qtdKEY):
        if(oldQTDItem is None and itemQTDDict is not None):
            return itemQTDDict.get(qtdKEY, None)
        else:
            return oldQTDItem
        