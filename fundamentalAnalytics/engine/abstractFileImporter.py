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
from tools.tools import getValueWithTagDict, getValueAsDate, \
    getDaysBetweenDates, getXSDFileFromCache, \
    getBinaryFileFromCache
from valueobject.constant import Constant
from valueobject.valueobject import FactVO, FactValueVO


class AbstractFileImporter():
    
    def isPeriodAllowed(self, element):
        periodDict = self.processCache[Constant.PERIOD_DICT]
        if (periodDict.get(element["@contextRef"], -1) != -1):
            return True
        return False    
    
    def getElementValue(self, xmlDict, elementID, attrID, periodDict):
        element = xmlDict[elementID]
        value = None
        if isinstance(element, list):
            for ele in element:
                if (self.isPeriodAllowed(ele)):
                    value0 = ele[attrID]
                    if(value is not None and value0 is not None):
                        raise Exception("Duplicated Value " + elementID + " " + attrID)
                        value = value0
        return value
    
    def getPeriodDict(self, xmlDictRoot, session): 
        periodDict = {}
        for item in getValueWithTagDict(Constant.XBRL_CONTEXT, xmlDictRoot):
            entityElement = getValueWithTagDict(Constant.XBRL_ENTITY, item)
            if(getValueWithTagDict(Constant.XBRL_SEGMENT, entityElement, False) == -1):
                documentPeriodEndDate = getValueAsDate(Constant.DOCUMENT_PERIOD_END_DATE, xmlDictRoot)['#text']
                documentPeriodEndDate = datetime.strptime(documentPeriodEndDate, '%Y-%m-%d')
                periodElement = getValueWithTagDict(Constant.XBRL_PERIOD, item)
                startDate = getValueAsDate(Constant.XBRL_START_DATE, periodElement)
                endDate = getValueAsDate(Constant.XBRL_END_DATE, periodElement)
                instant = getValueAsDate(Constant.XBRL_INSTANT, periodElement) 
                if(endDate != -1 and getDaysBetweenDates(documentPeriodEndDate, endDate) < 5):
                    #if(85 < getDaysBetweenDates(startDate, documentPeriodEndDate) < 95):
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
                            logging.getLogger('addToDB').debug("ADDED period " + str(period.startDate) + " " + str(period.endDate))
                        periodDict[item['@id']] = period
                elif(getDaysBetweenDates(instant, documentPeriodEndDate) < 5):
                    try:
                        period =  GenericDao.getOneResult(Period, and_(Period.instant == instant), session)
                    except NoResultFound:
                        period = Period()
                        period.instant = instant
                        session.add(period)
                        session.flush()
                        logging.getLogger('addToDB').debug("ADDED period " + str(period.instant))
                    periodDict[item['@id']] = period
        return periodDict

    def initProcessCache(self, filename, session):
        processCache = {}
        schDF = pandas.DataFrame(getValueWithTagDict(Constant.ELEMENT, getValueWithTagDict(Constant.SCHEMA, self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_SCH))))
        schDF.set_index("@id", inplace=True)
        schDF.head()
        processCache[Constant.DOCUMENT_SCH] = schDF
        #XML INSTANCE
        insDict = self.getXMLDictFromGZCache(filename, Constant.DOCUMENT_INS)
        insDict = getValueAsDate(Constant.XBRL_ROOT, insDict)
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
        
        CIK = insDict['dei:EntityCentralIndexKey']['#text']
        
        self.company = GenericDao.getOneResult(Company,Company.CIK.__eq__(CIK), session)
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
        logging.getLogger('general').debug("REPORT LIST " + str(reportDict))
        return reportDict
    
    
    def getUnitDict(self, xmlDictRoot):
        unitDict = {}
        for item in getValueWithTagDict(Constant.UNIT, xmlDictRoot):
            if (getValueWithTagDict(Constant.MEASURE, item, False) == -1):
                unitDict[item['@id']]
    
    def getFactByReport(self, reportDict, processCache, session):
        factToAddList = []
        #Obtengo para cada reporte sus conceptos
        xmlDictPre = processCache[Constant.DOCUMENT_PRE]
        for item in getValueWithTagDict(Constant.PRESENTATON_LINK, getValueWithTagDict(Constant.LINKBASE, xmlDictPre)): 
            reportRole = item['@xlink:role']
            if any(reportRole in s for s in reportDict.keys()):
                for item2 in getValueWithTagDict(Constant.LOC, item):
                    factVO = FactVO()
                    factVO.xlink_href = item2["@xlink:href"]
                    factVO.report = reportDict[reportRole]
                    factVO.labelID = item2["@xlink:label"]
                    factVO = self.setXsdValue(factVO, processCache)
                    if factVO.abstract != "true":
                        factToAddList.append(factVO)
                    else:
                        factVO = Dao.addAbstractConcept(factVO, session)
                for item2 in getValueWithTagDict(Constant.PRESENTATIONARC, item):
                    try:
                            if(item2["@xlink:arcrole"] == "http://www.xbrl.org/2003/arcrole/parent-child"):
                                objectTo = item2["@xlink:to"]
                                for factVO in factToAddList:
                                    if (factVO.labelID == objectTo
                                        and factVO.report == reportDict[reportRole]):
                                        factVO.order = item2["@order"]
                    except Exception as e:
                        print(e)
        return factToAddList
    
    
    
    def setXsdAttr(self, factVO, xsdDF, conceptID):
        factVO.conceptName = xsdDF.loc[conceptID]["@name"]
        factVO.periodType = xsdDF.loc[conceptID]["@xbrli:periodType"]
        factVO.balance = xsdDF.loc[conceptID]["@xbrli:balance"]
        factVO.type = xsdDF.loc[conceptID]["@type"]
        factVO.abstract = xsdDF.loc[conceptID]["@abstract"]
        return factVO
    
    def setXsdValue(self, factVO, processCache):    
        xsdURL = factVO.getXsdURL()
        conceptID = factVO.getConceptID()
        if(xsdURL[0:4] == "http"):
            xsdFileName = xsdURL[xsdURL.rfind("/") + 1: len(xsdURL)]
            if(isinstance(processCache.get(xsdFileName, -1), int)):
                xsdFile = getXSDFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//xsd//" + xsdFileName, xsdURL)
                xsdDict = xmltodict.parse(xsdFile)
                xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
                xsdDF.set_index("@id", inplace=True)
                xsdDF.head()
                processCache[xsdFileName] = xsdDF
            else:
                xsdDF = processCache.get(xsdFileName, -1)
            factVO = self.setXsdAttr(factVO, xsdDF, conceptID)
        else:
            xsdDF = processCache[Constant.DOCUMENT_SCH]
            factVO = self.setXsdAttr(factVO, xsdDF, conceptID)
        return factVO
    
    def getFactValue(self, periodDict, element):
        if(periodDict.get(element["@contextRef"], -1) != -1):
            factValue = FactValueVO()
            contextRef = element["@contextRef"]
            factValue.period = periodDict[contextRef]
            factValue.unitRef = element["@unitRef"]
            factValue.value = element["#text"]
            return factValue
    
    def setFactValues(self, factToAddList, processCache):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        periodDict = processCache[Constant.PERIOD_DICT]
        logging.getLogger('general').debug("periodDict " + str(periodDict))
        
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
                logging.getLogger('Error').debug("KeyError " + str(e) + " " + conceptID )
        return factToAddList
    
    def getXMLDictFromGZCache(self, filename, documentName):
        finalFileName = "C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename[0: filename.find(".txt")] + "/" + documentName + ".gz"
        logging.getLogger('general').debug("XML - Processing filename " + finalFileName.replace("//", "/"))
        file = getBinaryFileFromCache(finalFileName)
        if (file is not None):
            with gzip.open(BytesIO(file), 'rb') as f:
                file_content = f.read()
                text = file_content.decode("ISO-8859-1")
                xmlDict = xmltodict.parse(text)
                return xmlDict
        else:
            return None