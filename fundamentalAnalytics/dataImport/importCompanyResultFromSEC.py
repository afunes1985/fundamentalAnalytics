'''
Created on 22 ago. 2018

@author: afunes
'''
from _decimal import Decimal, InvalidOperation
from _io import BytesIO, StringIO
from datetime import timedelta, datetime
from enum import Enum
import gzip
import logging
import os
from pathlib import Path

from pandas.core.frame import DataFrame
import requests
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_, or_, exists
import xmltodict 

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.fact import Fact
from modelClass.period import QuarterPeriod, Period
from modelClass.report import Report
import pandas as pd
from tools.tools import getValueWithTagDict, tagNameAlias, getXmlDictFromText, \
    getValueAsDate, getDaysBetweenDates, getTxtFileFromCache, \
    getXSDFileFromCache, getBinaryFileFromCache, createLog
from valueobject.valueobject import ContextRef, FactVO, FactValueVO


def getUnitDict(xmlDictRoot):
    unitDict = {}
    for item in getValueWithTagDict(tagNameAlias['UNIT'], xmlDictRoot):
        if (getValueWithTagDict(tagNameAlias['MEASURE'], item, False) == -1):
            unitDict[item['@id']]

def getPeriodDict(xmlDictRoot): 
    periodDict = {}
    for item in getValueWithTagDict(tagNameAlias['XBRL_CONTEXT'], xmlDictRoot):
        entityElement = getValueWithTagDict(tagNameAlias['XBRL_ENTITY'], item)
        if(getValueWithTagDict(tagNameAlias['XBRL_SEGMENT'], entityElement, False) == -1):
            documentPeriodEndDate = getValueAsDate('DOCUMENT_PERIOD_END_DATE', xmlDictRoot)['#text']
            documentPeriodEndDate = datetime.strptime(documentPeriodEndDate, '%Y-%m-%d')
            periodElement = getValueWithTagDict(tagNameAlias['XBRL_PERIOD'], item)
            startDate = getValueAsDate('XBRL_START_DATE', periodElement)
            endDate = getValueAsDate('XBRL_END_DATE', periodElement) 
            instant = getValueAsDate('XBRL_INSTANT', periodElement) 
            if(endDate != -1 and getDaysBetweenDates(documentPeriodEndDate, endDate) < 5):
                if(85 < getDaysBetweenDates(startDate, documentPeriodEndDate) < 95):
                    try:
                        period =  GenericDao.getOneResult(Period, and_(Period.startDate == startDate, Period.endDate == endDate), session)
                    except NoResultFound:
                        period = Period()
                        period.startDate = startDate
                        period.endDate = endDate
                        session.add(period)
                        session.flush()
                        logging.getLogger('addToDB').debug("ADDED period " + period.startDate + " " + period.endDate)
                    periodDict[item['@id']] = period
            elif(getDaysBetweenDates(instant, documentPeriodEndDate) < 5):
                try:
                    period =  GenericDao.getOneResult(Period, and_(Period.instant == instant), session)
                except NoResultFound:
                    period = Period()
                    period.instant = instant
                    session.add(period)
                    session.flush()
                    logging.getLogger('addToDB').debug("ADDED period " + period.instant)
                periodDict[item['@id']] = period
#         periodElement = getValueWithTagDict(tagNameAlias['XBRL_PERIOD'], item)
#         startDate = getValueAsDate('XBRL_START_DATE', periodElement)
#         endDate = getValueAsDate('XBRL_END_DATE', periodElement) 
#         instant = getValueAsDate('XBRL_INSTANT', periodElement)
#         entityElement = getValueWithTagDict(tagNameAlias['XBRL_ENTITY'], item)
#         if(getValueWithTagDict(tagNameAlias['XBRL_SEGMENT'], entityElement, False) == -1):
#             documentPeriodEndDate = getValueAsDate('DOCUMENT_PERIOD_END_DATE', xmlDictRoot)['#text']
#             documentPeriodEndDate = datetime.strptime(documentPeriodEndDate, '%Y-%m-%d')
#             #logging.getLogger('general').debug("item " + str(item))
#             if(getDaysBetweenDates(instant, documentPeriodEndDate) < 5):
#                 contextRefDict["FIX"] = id_
#             elif(getDaysBetweenDates(documentPeriodEndDate, endDate) < 5):
#                 #logging.getLogger('general').debug("item " + str(item))
#                 endDatePreviousYear = documentPeriodEndDate.replace(year = documentPeriodEndDate.year -1, month = 12, day = 31)
#                 if(getDaysBetweenDates(startDate, endDatePreviousYear) < 5):
#                     contextRefDict["YTD"] = id_
#                 elif(85 < getDaysBetweenDates(startDate, documentPeriodEndDate) < 95):
#                     contextRefDict["QTD"] = id_
    return periodDict

def getReportDict(fileText):
    #Obtengo reportes statements
    xmlDict= getXmlDictFromText(fileText, "FILENAME", "FilingSummary.xml", "XML")
    reportDict = {}
    for report in xmlDict["FilingSummary"]["MyReports"]["Report"]:
        if(report.get("MenuCategory", -1) == "Statements"):
            try:
                reportRole = report["Role"]
                reportShortName = report["ShortName"]
                report =  GenericDao.getOneResult(Report, and_(Report.shortName == reportShortName), session)
            except NoResultFound:
                report = Report()
                report.shortName = reportShortName
                session.add(report)
                session.flush()
                logging.getLogger('addToDB').debug("ADDED report " + reportShortName)
            reportDict[reportRole] = report
    logging.getLogger('general').debug("REPORT LIST " + str(reportDict))
    return reportDict

def getFactByReport(fileText, reportDict):
    factToAddList = []
    #Obtengo para cada reporte sus conceptos
    xmlDict2= getXmlDictFromText(fileText, "TYPE", "EX-101.PRE", "XBRL")
    for item in getValueWithTagDict(tagNameAlias['PRESENTATON_LINK'], getValueWithTagDict(tagNameAlias['LINKBASE'], xmlDict2)): 
        reportRole = item['@xlink:role']
        if any(reportRole in s for s in reportDict.keys()):
            for item2 in getValueWithTagDict(tagNameAlias['LOC'], item):
                factVO = FactVO()
                factVO.xlink_href = item2["@xlink:href"]
                factVO.report = reportDict[reportRole]
                factToAddList.append(factVO)
    return factToAddList

def initProcessCache(fileText):
    processCache = {}
    xmlDict3= getXmlDictFromText(fileText, "TYPE", "EX-101.SCH", "XBRL")
    xsdDF = pd.DataFrame(getValueWithTagDict(tagNameAlias['ELEMENT'], getValueWithTagDict(tagNameAlias['SCHEMA'], xmlDict3)))
    xsdDF.set_index("@id", inplace=True)
    xsdDF.head()
    processCache["EX-101.SCH"] = xsdDF
    return processCache
    
    
def importSECFile(filenameList, replace, company, session):
        for filename in filenameList:
            logging.getLogger('general').debug("START - Processing filename " + filename)
            fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
            reportDict = getReportDict(fileText)
            factToAddList = getFactByReport(fileText, reportDict)
            processCache = initProcessCache(fileText)
            
            listToDelete = []
            for factVO in factToAddList:
                xsdURL = factVO.getXsdURL()
                conceptID = factVO.getConceptID()
                #logging.getLogger('bodyXML').debug(xsdURL)
                if(xsdURL[0:4] == "http"):
                    xsdFileName = xsdURL[xsdURL.rfind("/") + 1: len(xsdURL)]
                    #logging.getLogger('bodyXML').debug(xsdFileName)
                    if(isinstance(processCache.get(xsdFileName, -1), int)):
                        xsdFile = getXSDFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//xsd//" + xsdFileName, xsdURL)
                        xsdDict = xmltodict.parse(xsdFile)
                        xsdDF = pd.DataFrame(xsdDict["xs:schema"]["xs:element"])
                        xsdDF.set_index("@id", inplace=True)
                        xsdDF.head()
                        processCache[xsdFileName] = xsdDF
                    else:
                        xsdDF = processCache.get(xsdFileName, -1)
                    factVO.conceptName = xsdDF.loc[conceptID]["@name"]
                    factVO.periodType = xsdDF.loc[conceptID]["@xbrli:periodType"]
                    factVO.balance = xsdDF.loc[conceptID]["@xbrli:balance"]
                    factVO.type = xsdDF.loc[conceptID]["@type"]
                    factVO.abstract = xsdDF.loc[conceptID]["@abstract"]
                    if(factVO.abstract == "true"):
                        listToDelete.append(factVO)
                else:
                    xsdDF = processCache["EX-101.SCH"]
                    factVO.conceptName = xsdDF.loc[conceptID]["@name"]
                    factVO.periodType = xsdDF.loc[conceptID]["@xbrli:periodType"]
                    factVO.balance = xsdDF.loc[conceptID]["@xbrli:balance"]
                    factVO.type = xsdDF.loc[conceptID]["@type"]
                    factVO.abstract = xsdDF.loc[conceptID]["@abstract"]
                    if(factVO.abstract == "true"):
                        listToDelete.append(factVO)
                
            #DELETE ABSTRACT FACTS            
            factToAddList = [x for x in factToAddList if x not in listToDelete]
            
            xmlDictRoot = getXmlDictFromText(fileText,"TYPE","EX-101.INS","XBRL")
            xmlDictRoot = getValueAsDate('XBRL_ROOT', xmlDictRoot)
            CIK = xmlDictRoot['dei:EntityCentralIndexKey']['#text']
            logging.getLogger('general').debug("CIK " + CIK)
            documentType = xmlDictRoot['dei:DocumentType']['#text']
            logging.getLogger('general').debug("documentType " + documentType)
            periodDict = getPeriodDict(xmlDictRoot)
            logging.getLogger('general').debug("periodDict " + str(periodDict))
            
            for factVO in factToAddList:
                    conceptID = factVO.xlink_href[factVO.xlink_href.find("#", 0) + 1:len(factVO.xlink_href)]
                    try:
                        element = xmlDictRoot[conceptID.replace("_", ":")]
                        #print (element)
                        if isinstance(element, list):
                            for element1 in element:
                                if(periodDict.get(element1["@contextRef"], -1) != -1):
                                    factValue = FactValueVO()
                                    contextRef = element1["@contextRef"]
                                    factValue.period = periodDict[contextRef]
                                    factValue.unitRef = element1["@unitRef"]
                                    factValue.value = element1["#text"]
                                    factVO.factValueList.append(factValue)
                        else:
                            if(periodDict.get(element1["@contextRef"], -1) != -1):
                                factValue = FactValueVO()
                                contextRef = element1["@contextRef"]
                                factValue.period = periodDict[contextRef]
                                factValue.unitRef = element["@unitRef"]
                                factValue.value = element["#text"]
                                factVO.factValueList.append(factValue)
                    except KeyError:
                        logging.getLogger('bodyXML').debug("Context Ref Not Found " + contextRef)
                
            for factVO in factToAddList:
                logging.getLogger('bodyXML').debug(factVO.report.shortName + " " + factVO.getConceptID() + " " + str(factVO.factValueList)) 
            addFact(factToAddList, company, session)
        logging.getLogger('general').debug("END - Processing filename " + filename)

def addFact(factToAddList, company, session):
    for factVO in factToAddList:
        try:
            concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(factVO.conceptName), session)
        except NoResultFound:
            logging.getLogger('general').debug("Add " + factVO.conceptName)
            concept = Concept()
            concept.conceptID = factVO.conceptName
            session.add(concept)
            session.flush()
        try:
            factToAdd = GenericDao.getOneResult(Fact, and_(Fact.company == company, Fact.concept == concept), session)
        except NoResultFound:
            factToAdd = Fact()
        factToAdd.company = company
        factToAdd.concept = concept
        factToAdd.report = factVO.report
        if (len(factVO.factValueList)== 1):
            factToAdd.value = factVO.factValueList[0].value
            factToAdd.period = factVO.factValueList[0].period
            if(factVO.factValueList[0].contextRef is not None):
                print(factVO.factValueList[0].contextRef)
        session.add(factToAdd)
        logging.getLogger('addToDB').debug("Added " + str(factVO.conceptName))
    session.commit()

def readSECIndexFor(period, company, replace):
    logging.getLogger('general').debug("START - Processing index file " + company.ticker  + " " + str(period.year) + "-" +  str(period.quarter) + " " + " replace " + str(replace))   
    file = getBinaryFileFromCache('C://Users//afunes//iCloudDrive//PortfolioViewer//cache//master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/master.gz")
    with gzip.open(BytesIO(file), 'rb') as f:
        file_content = f.read()
        text = file_content.decode("ISO-8859-1")
        text = text[text.find("CIK", 0, len(text)): len(text)]
        point1 = text.find("\n")
        point2 = text.find("\n", point1+1)
        text2 = text[0:point1] + text[point2 : len(text)]
        df = pd.read_csv(StringIO(text2), sep="|")
        df.set_index("CIK", inplace=True)
        df.head()
        #logging.getLogger('bodyIndex').debug(df.to_string())
        rowData0 = df.loc[company.CIK]
        if isinstance(rowData0, DataFrame):
            for rowData1 in rowData0.iterrows():
                filename = rowData1[1]["Filename"]
                formType = rowData1[1]["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):
                    importSECFile([filename], replace, company, session)
        else:
            filename = rowData0["Filename"]
            formType = rowData0["Form Type"]
            if(formType == "10-Q" or formType == "10-K"):
                importSECFile([filename], replace, company, session)
    logging.getLogger('general').debug("END - Processing index file " + company.ticker  + " " + str(period.year) + "-" +  str(period.quarter) + " " + " replace " + str(replace)) 

COMPANY_TICKER = 'MSFT'
replace = True
Initializer()
session = DBConnector().getNewSession()
company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
#periodList = objectResult = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2018, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter <= 3)), QuarterPeriod.year > 2016)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
periodList = objectResult = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 2)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
createLog('general', logging.DEBUG)
createLog('bodyIndex', logging.INFO)
createLog('bodyXML', logging.DEBUG)
createLog('InvalidOperation_convertToDecimal', logging.DEBUG)
createLog('notinclude_UnitRef', logging.INFO)
createLog('notinclude_ContextRef', logging.INFO)
createLog('skipped_underscore', logging.INFO)
createLog('skipped', logging.INFO)
createLog('xsdNotFound', logging.DEBUG)
createLog('addToDB', logging.INFO)
createLog('matchList_empty', logging.INFO)
createLog('tempData', logging.INFO)
logging.info("START")

for period in periodList:
    readSECIndexFor(period, company, replace)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
#     
#     
# class YearStrategy(Enum):
#     FISCAL_YEAR = 1
#     DOC_PERIOD_END_DATE = 2
#     ERROR = 3
#     
    
#     
#     
#     
# def addResultData(resultList, company, period, session):
#     for cqr in resultList:
#         try:
#             concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(cqr.conceptID), session)
#         except NoResultFound:
#             logging.getLogger('general').debug("Add " +cqr.conceptID)
#             concept = Concept()
#             concept.conceptID = cqr.conceptID
#             session.add(concept)
#             session.flush()
#         try:
#             cqrToAdd = GenericDao.getOneResult(CompanyQResult, and_(CompanyQResult.company == company,  CompanyQResult.period == period, CompanyQResult.concept == concept, CompanyQResult.periodType == cqr.periodType), session)
#         except NoResultFound:
#             cqrToAdd = CompanyQResult()
#         cqrToAdd.company = company
#         cqrToAdd.concept = concept
#         cqrToAdd.period = period
#         cqrToAdd.value = cqr.value
#         cqrToAdd.periodType = cqr.periodType
#         session.add(cqrToAdd)
#         logging.getLogger('addResultData').debug("Added " + str(cqr.conceptID) + " " + cqr.periodType)
#     session.commit()
    

# def getCQRFromElement(element, tagName, contextRefDict):
#         contextRefToUseFI = contextRefDict.get("FIX", -1)
#         contextRefToUseFDQTD = contextRefDict.get("QTD", -1)
#         contextRefToUseFDYTD = contextRefDict.get("YTD", -1)
#         conceptID = tagName[8:len(tagName)]
#         #Si tiene context ref
#         if(element.get('@contextRef', -1) != -1):
#             contextRef = element['@contextRef']
#             cqr = CompanyQResult()
#             cqr.conceptID = conceptID
#             cqr.value = getValueFromElement(element, conceptID)
#             cqr.row = element
#             #Si conextRef es igual a FI2018Q2
#             if(contextRef == contextRefToUseFI):
#                 cqr.periodType = "FIX"
#             #Si contextRef es igual a FD2018Q2QTD
#             elif(contextRef == contextRefToUseFDQTD):
#                 cqr.periodType = "QTD"
#             #Si contextRef es igual a FD2018Q2QTD
#             elif(contextRef == contextRefToUseFDYTD):
#                 cqr.periodType = "YTD"
#             else:
#                 raise LoggingException('skipped', "Skipped concept for " + contextRefToUseFI + " " + tagName + " " + str(element))
#             return cqr
#         else:
#             raise LoggingException('notinclude_ContextRef', str(element))

# def getCQRListFromTagName(xmlDictRoot, tagName, contextRefDict):
#     resultList = []
#     if isinstance(xmlDictRoot[tagName], list):
#         for xmlElement in xmlDictRoot[tagName]:
#             try:
#                 cqr =  getCQRFromElement(element = xmlElement, tagName = tagName, contextRefDict = contextRefDict)
#                 logging.getLogger('tempData').debug("Add to list " + cqr.conceptID)
#                 resultList.append(cqr)
#             except LoggingException as le:
#                 le.log()
#     else:
#         try:
#             cqr =  getCQRFromElement(element = xmlDictRoot[tagName], tagName = tagName, contextRefDict = contextRefDict)
#             resultList.append(cqr)
#         except LoggingException as le:
#             le.log()
#     if(not resultList):
#         logging.getLogger('matchList_empty').debug(tagName + " " + str(xmlDictRoot[tagName]))
#     return resultList

# 
# def getCQRListFromXML(xmlDictRoot, contextRefDict):
#     qrcResultList = []
#     for tagName in xmlDictRoot:
#         if(tagName[0:8] == "us-gaap:" and tagName.find("TextBlock") == -1):
#             returnList = getCQRListFromTagName(xmlDictRoot, tagName, contextRefDict)
#             if (returnList is not None):
#                 qrcResultList = qrcResultList + returnList
#     return qrcResultList

# def getFiscalYear(documentPeriodYear, fiscalYear, yearStrategy):
#     if(documentPeriodYear != fiscalYear):
#         if (yearStrategy == YearStrategy.ERROR):
#             raise Exception('FISCAL YEAR IS DIF THAN PERIOD YEAR ' + fiscalYear + " " + documentPeriodYear)
#         else:
#             logging.getLogger('general').warning('FISCAL YEAR IS DIF THAN PERIOD YEAR ' + fiscalYear + " " + documentPeriodYear)
#             fiscalYearToUse = fiscalYear
#     if(yearStrategy == YearStrategy.FISCAL_YEAR):
#         fiscalYearToUse = fiscalYear
#     elif(yearStrategy == YearStrategy.DOC_PERIOD_END_DATE):
#         fiscalYearToUse = documentPeriodYear
#     else:
#         fiscalYearToUse = fiscalYear
#     return fiscalYearToUse
# 
# 
# def getFiscalYear2(xmlDictRoot):
#     fiscalYear = getValueWithTagDict(tagNameAlias['DOCUMENT_FISCAL_YEAR_FOCUS'], xmlDictRoot)['#text']
#     logging.getLogger('general').debug("fiscalYear " + fiscalYear)
#     documentPeriodEndDate = getValueWithTagDict(tagNameAlias['DOCUMENT_PERIOD_END_DATE'], xmlDictRoot)['#text']
#     logging.getLogger('general').debug("documentPeriodEndDate " + documentPeriodEndDate)
#     documentPeriodYear = documentPeriodEndDate[0:4]
#     return getFiscalYear(documentPeriodYear, fiscalYear, yearStrategy)
# 
# def getFiscalPeriod(xmlDictRoot):
#     fiscalPeriod = getValueWithTagDict(tagNameAlias['DOCUMENT_FISCAL_PERIOD_FOCUS'], xmlDictRoot)['#text']
#     fiscalPeriod = fiscalPeriod[1:2]
#     logging.getLogger('general').debug("fiscalPeriod " + fiscalPeriod)
#     if (fiscalPeriod =='Y'):
#         fiscalPeriod = 4
#     return fiscalPeriod

# def getValueFromElement(xmlElement, conceptID):
#     value = None
#     value0 = None
#     #Valida que este dentro de las unidades permitidas (USD, SHARES, ETC)
#     if(xmlElement.get('@unitRef', -1) != -1):
#         if any(xmlElement['@unitRef'] in s for s in unitRefList):
#             value0 = xmlElement.get('#text', -1)
#         else:
#             raise LoggingException('skipped', "unitRef not valid "  + conceptID + " " + str(xmlElement))
#     else:
#         raise LoggingException('notinclude_UnitRef', "Not unitRef " + conceptID + " " + str(xmlElement))
#     try:
#         if(value0 is not None):
#             value = Decimal(value0)
#             return value
#     except InvalidOperation:
#         raise LoggingException('InvalidOperation_convertToDecimal', "Error formating field " + str(xmlElement))

#unitRefList = ['usd', 'number', 'shares', 'usdPerShare', 'U_iso4217USD', 'U_shares', 'U_iso4217USD_shares']