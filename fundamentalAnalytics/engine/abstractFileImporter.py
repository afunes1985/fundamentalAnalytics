'''
Created on 19 sep. 2018

@author: afunes
'''
from _io import BytesIO
from datetime import datetime
import gzip
import logging

import pandas
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_
import xmltodict

from dao.dao import GenericDao, Dao
from modelClass.company import Company
from modelClass.period import Period
from tools.tools import getDaysBetweenDates, getXSDFileFromCache, getBinaryFileFromCache,\
    FileNotFoundException
from valueobject.constant import Constant
from valueobject.valueobject import FactVO, FactValueVO
from pandas.core.series import Series


class AbstractFileImporter():
    
    def isPeriodDefaultAllowed(self, element):
        if(self.getElementFromElement(Constant.XBRL_SEGMENT, element, False) is None):
            return True
        return False   
    
    def isPeriodAllowed(self, element):
        if (self.processCache is not None):
            periodDict = self.processCache[Constant.PERIOD_DICT]
            if (periodDict is not None and periodDict.get(element["@contextRef"], None) is not None):
                return True
            elif(self.isPeriodDefaultAllowed(element)):## VER SI SE DEBE BORRAR
                return True
            return False  
        elif(self.isPeriodDefaultAllowed(element)):
                return True
        else:
            return False 
    
    def getPeriodDict(self, xmlDictRoot, session): 
        periodDict = {}
        documentPeriodEndDate = self.getValueAsDate(['#text'], self.getObjectFromElement(Constant.DOCUMENT_PERIOD_END_DATE, xmlDictRoot))
        for item in self.getListFromElement(Constant.XBRL_CONTEXT, xmlDictRoot):
            entityElement = self.getElementFromElement(Constant.XBRL_ENTITY, item)
            if(self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement, False) is None):
                periodElement = self.getElementFromElement(Constant.XBRL_PERIOD, item)
                startDate = self.getValueAsDate(Constant.XBRL_START_DATE, periodElement)
                endDate = self.getValueAsDate(Constant.XBRL_END_DATE, periodElement)
                instant = self.getValueAsDate(Constant.XBRL_INSTANT, periodElement) 
                if(endDate is not None and getDaysBetweenDates(documentPeriodEndDate, endDate) < 5):
                        try:
                            period =  GenericDao.getOneResult(Period, and_(Period.startDate == startDate, Period.endDate == endDate), session)
                            period.daysBetween = getDaysBetweenDates(startDate, endDate)
                        except NoResultFound:
                            period = Period()
                            period.startDate = startDate
                            period.endDate = endDate
                            period.daysBetween = getDaysBetweenDates(startDate, endDate)
                            session.add(period)
                            session.flush()
                            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added period " + str(period.startDate) + " " + str(period.endDate))
                        periodDict[item['@id']] = period
                elif(getDaysBetweenDates(instant, documentPeriodEndDate) < 5):
                    try:
                        period =  GenericDao.getOneResult(Period, and_(Period.instant == instant), session)
                    except NoResultFound:
                        period = Period()
                        period.instant = instant
                        session.add(period)
                        session.flush()
                        logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added period " + str(period.instant))
                    periodDict[item['@id']] = period
        return periodDict

    def initProcessCache(self, filename, session):
        processCache = {}
        schDF = pandas.DataFrame(self.getListFromElement(Constant.ELEMENT, self.getElementFromElement(Constant.SCHEMA, self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_SCH))))
        schDF.set_index("@id", inplace=True)
        schDF.head()
        processCache[Constant.DOCUMENT_SCH] = schDF
        #XML INSTANCE
        insDict = self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_INS)
        insDict = self.getElementFromElement(Constant.XBRL_ROOT, insDict)
        processCache[Constant.DOCUMENT_INS] = insDict
        #XML SUMMARY
        sumDict= self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_SUMMARY)
        processCache[Constant.DOCUMENT_SUMMARY] = sumDict
        #XML PRESENTATION
        preDict = self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_PRE)
        processCache[Constant.DOCUMENT_PRE] = preDict 
        #PERIOD
        periodDict = self.getPeriodDict(insDict, session)
        processCache[Constant.PERIOD_DICT] = periodDict 
        #COMPANY
        CIK = self.getValueFromElement(['#text'], insDict['dei:EntityCentralIndexKey'])
        try:
            self.company = GenericDao.getOneResult(Company,Company.CIK.__eq__(CIK), session)
        except NoResultFound:
            self.company = Company()
            self.company.CIK = CIK
            session.add(self.company)
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added Company" + str(CIK))
            session.commit()
        return processCache
    
    
    def getReportDict(self, processCache, session):
        #Obtengo reportes statements
        xmlDict= processCache[Constant.DOCUMENT_SUMMARY]
        reportDict = {}
        for report in xmlDict["FilingSummary"]["MyReports"]["Report"]:
            if(report.get("MenuCategory", -1) == "Statements"):
                reportRole = report["Role"]
                reportShortName = report["ShortName"]
                report = Dao.getReport(reportShortName, session)
                reportDict[reportRole] = report
        logging.getLogger(Constant.LOGGER_GENERAL).debug("REPORT LIST " + str(reportDict))
        return reportDict
    
    
    def getUnitDict(self, xmlDictRoot):
        unitDict = {}
        for item in self.getListFromElement(Constant.UNIT, xmlDictRoot):
            if (self.getElementFromElement(Constant.MEASURE, item, False) == -1):
                unitDict[item['@id']]
    
    def getFactByReport(self, reportDict, processCache, session):
        factToAddList = []
        #Obtengo para cada reporte sus conceptos
        xmlDictPre = processCache[Constant.DOCUMENT_PRE]
        for item in self.getListFromElement(Constant.PRESENTATON_LINK, self.getElementFromElement(Constant.LINKBASE, xmlDictPre)): 
            reportRole = item['@xlink:role']
            if (reportDict.get(reportRole, None) is not None):
                presentationDF = pandas.DataFrame(self.getListFromElement(Constant.PRESENTATIONARC, item))
                presentationDF.set_index("@xlink:to", inplace=True)
                presentationDF.head() 
                for item2 in self.getListFromElement(Constant.LOC, item):
                    factVO = FactVO()
                    factVO.xlink_href = item2["@xlink:href"]
                    factVO.report = reportDict[reportRole]
                    factVO.labelID = item2["@xlink:label"]
                    factVO = self.setXsdValue(factVO, processCache)
                    if factVO.abstract != "true":
                        try:
                            factVO.order = self.getValueFromElement( ["@order"], presentationDF.loc[factVO.labelID], True) 
                            factToAddList.append(factVO)
                        except Exception as e:
                            logging.getLogger(Constant.LOGGER_ERROR).debug("Error " + str(e))
                        
        return factToAddList
    
    def setXsdAttr(self, factVO, xsdDF, conceptID):
        try:
            factVO.conceptName = self.getValueFromElement(["@name"], xsdDF.loc[conceptID]) 
            factVO.periodType = self.getValueFromElement(["@xbrli:periodType"], xsdDF.loc[conceptID], False) 
            factVO.balance = self.getValueFromElement(["@xbrli:balance"], xsdDF.loc[conceptID], False)
            factVO.type = self.getValueFromElement(["@type"], xsdDF.loc[conceptID], False) 
            factVO.abstract = self.getValueFromElement(["@abstract"], xsdDF.loc[conceptID], False)
        except Exception as e:
            raise e    
        return factVO
    
    def setXsdValue(self, factVO, processCache):    
        xsdURL = factVO.getXsdURL()
        conceptID = factVO.getConceptID()
        if(xsdURL[0:4] == "http"):
            xsdFileName = xsdURL[xsdURL.rfind("/") + 1: len(xsdURL)]
            if(processCache.get(xsdFileName, None) is None):
                xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, xsdURL)
                xsdDict = xmltodict.parse(xsdFile)
                xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
                xsdDF.set_index("@id", inplace=True)
                xsdDF.head()
                processCache[xsdFileName] = xsdDF
            else:
                xsdDF = processCache.get(xsdFileName, None)
            factVO = self.setXsdAttr(factVO, xsdDF, conceptID)
        else:
            xsdDF = processCache[Constant.DOCUMENT_SCH]
            factVO = self.setXsdAttr(factVO, xsdDF, conceptID)
        return factVO
    
    def getFactValue(self, periodDict, element):
        if(periodDict.get(element["@contextRef"], None) is not None):
            factValue = FactValueVO()
            contextRef = element["@contextRef"]
            factValue.period = periodDict[contextRef]
            factValue.unitRef = element["@unitRef"]
            factValue.value = element["#text"]
            return factValue
    
    def setFactValues(self, factToAddList, processCache):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        periodDict = processCache[Constant.PERIOD_DICT]
        logging.getLogger(Constant.LOGGER_GENERAL).debug("periodDict " + str(periodDict))
        
        for factVO in factToAddList:
            conceptID = factVO.xlink_href[factVO.xlink_href.find("#", 0) + 1:len(factVO.xlink_href)]
            try:
                element = insXMLDict[conceptID.replace("_", ":")]
                if isinstance(element, list):
                    for element1 in element:
                        factValue = self.getFactValue(periodDict, element1)
                        if (factValue is not None):
                            factVO.factValueList.append(factValue)
                else:
                    factValue = self.getFactValue(periodDict, element)
                    if (factValue is not None):
                        factVO.factValueList.append(factValue)
            except KeyError as e:
                logging.getLogger(Constant.LOGGER_ERROR).debug("KeyError " + str(e) + " " + conceptID )
        return factToAddList
    
    def getXMLDictFromGZCache(self, filename, documentName):
        finalFileName = Constant.CACHE_FOLDER + filename[0: filename.find(".txt")] + "/" + documentName + ".gz"
        logging.getLogger(Constant.LOGGER_GENERAL).debug("XML - Processing filename " + finalFileName.replace("//", "/"))
        file = getBinaryFileFromCache(finalFileName)
        if (file is not None):
            with gzip.open(BytesIO(file), 'rb') as f:
                file_content = f.read()
                text = file_content.decode("ISO-8859-1")
                xmlDict = xmltodict.parse(text)
                return xmlDict
        else:
            raise FileNotFoundException("File not found " + finalFileName.replace("//", "/"))
        
    def getValueAsDate(self, attrID, element):
        value = self.getValueFromElement(attrID, element, False)
        if(value is not None):
            return datetime.strptime(value, '%Y-%m-%d')
    
    def getObjectFromElement(self, objectIDList, element):
        objectsToReturn = []
        for objectID in objectIDList:
            if(element.get(objectID, None) is not None):
                objectsToReturn.append(element.get(objectID))
        if(len(objectsToReturn) == 1):
            return objectsToReturn[0]
        listToReturn = []
        for obj in objectsToReturn:
            if isinstance(obj, list):
                listToReturn = listToReturn + obj
        if (len(listToReturn) != 0):
            return listToReturn
        return None
            
    def getObjectFromList(self, objectIDList, list_):
        for objectID in objectIDList:
            for element in list_:
                if(element.get(objectID, None) is not None):
                    if(self.isPeriodAllowed(element)):
                        return element
    
    def getObjectFromSerie(self, objectIDList, serie):
        obj = None
        for objectID in objectIDList:
            obj2 = serie.get(objectID, None)
            if(obj2 is not None
               and obj2 == obj2):
                obj = obj2
        return obj
     
    def getListFromElement(self, elementIDList, element, raiseException = True):
        obj = self.getObjectFromElement(elementIDList, element)
        if (obj is None):
            if (raiseException):
                raise Exception("List for elementID not found "  + str(elementIDList) + " " +  str(element)[0:50])
        elif(isinstance(obj, dict)):
            return [obj]
        elif(not isinstance(obj, list)):
            raise Exception("List for elementID is not list "  + str(elementIDList) + " " +  str(element)[0:50])
        else:
            return obj
    
    def getElementFromElement(self, elementIDList, element, raiseException = True):
        obj = self.getObjectFromElement(elementIDList, element)
        if (obj is None):
            if (raiseException):
                raise Exception("Element for elementID not found "  + str(elementIDList) + " " +  str(element)[0:50])
        elif(not isinstance(obj, dict)):
            raise Exception("Element for elementID is not dict "  + str(elementIDList) + " " +  str(element)[0:50])
        else:
            return obj
    
    def getValueFromElement(self, attrIDList, element, raiseException = True):
        obj = None
        if(isinstance(element, dict)):
            obj = self.getObjectFromElement(attrIDList, element)
        elif(isinstance(element, list)):
            obj = self.getObjectFromList(attrIDList, element)
        elif(isinstance(element, Series)):
            obj = self.getObjectFromSerie(attrIDList, element)    
        if (obj is None):
            if (raiseException):
                raise Exception("Value for attrID not found "  + str(attrIDList) + " " +  str(element)[0:50])
        elif(isinstance(obj, dict)):
            return self.getValueFromElement(attrIDList, obj, raiseException)
        elif(not isinstance(obj, str) and obj is not None):
            raise Exception("Value for elementID is not str "  + str(attrIDList) + " " +  str(element)[0:50])
        else:
            return obj
