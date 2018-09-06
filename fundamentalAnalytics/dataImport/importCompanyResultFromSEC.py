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
from modelClass.companyqresult import CompanyQResult
from modelClass.concept import Concept
from modelClass.period import Period
import pandas as pd

class Fact():
    def __init__(self):
        self.xlink_href = None
        self.conceptName = None

class LoggingException(Exception):
    def __init__(self, loggerName, message):
        self.loggerName = loggerName
        self.message = message

    def log(self):
        logging.getLogger(self.loggerName).debug(self.message)

class YearStrategy(Enum):
    FISCAL_YEAR = 1
    DOC_PERIOD_END_DATE = 2
    ERROR = 3
    
unitRefList = ['usd', 'number', 'shares', 'usdPerShare', 'U_iso4217USD', 'U_shares', 'U_iso4217USD_shares']
tagNameAlias = { "DOCUMENT_FISCAL_PERIOD_FOCUS" : ['dei:DocumentFiscalPeriodFocus'],
                 "DOCUMENT_FISCAL_YEAR_FOCUS" : ['dei:DocumentFiscalYearFocus'],
                 "DOCUMENT_PERIOD_END_DATE" : ['dei:DocumentPeriodEndDate'],
                 "XBRL_ROOT" : ['xbrli:xbrl','xbrl'],
                 "XBRL_CONTEXT" :  ['xbrli:context','context'],
                 "XBRL_PERIOD" : ['xbrli:period','period'],
                 "XBRL_START_DATE" : ['xbrli:startDate','startDate'],
                 "XBRL_END_DATE" : ['xbrli:endDate','endDate'],
                 "XBRL_INSTANT" : ['xbrli:instant','instant'],
                 "XBRL_ENTITY" : ['xbrli:entity','entity'],
                 "XBRL_SEGMENT" : ['xbrli:segment','segment'],
                 "LINKBASE" : ['link:linkbase','linkbase'],
                 "PRESENTATON_LINK" : ["link:presentationLink","presentationLink"],
                 "LOC" : ["link:loc", "loc"]
                }

def getValueWithTagDict(tagnameList, element, raiseException = True):
    for tagname in tagnameList:
        if(element.get(tagname, -1) != -1):
            return element.get(tagname, -1)
    if (raiseException):
        raise Exception("Element for tagname not found "  + str(tagnameList) + " " +  str(element))
    else:
        return -1

def getBinaryFileFromCache(filename, url):
    xbrlFile = Path(filename)
    if xbrlFile.exists():
        with open(filename, mode='rb') as file: 
            file = file.read()
    else:
        response = requests.get(url, timeout = 30) 
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename, 'wb') as f:
            f.write(response.content)
            file = response.content
    return file

def getTxtFileFromCache(filename, url):
    xbrlFile = Path(filename)
    if xbrlFile.exists():
        with open(filename, mode='r') as file: 
            fileText = file.read()
    else:
        response = requests.get(url, timeout = 30) 
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename, 'wb') as f:
            f.write(response.content)
            fileText = response.text
    return fileText

def getXSDFileFromCache(filename, url):
    xbrlFile = Path(filename)
    if xbrlFile.exists():
        with open(filename, mode='r') as file: 
            fileText = file.read()
    else:
        response = requests.get(url, timeout = 30) 
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename, 'wb') as f:
            f.write(response.content)
            fileText = response.text
    return fileText

def createLog(logName, level):
    logger= logging.getLogger(logName)
    logger.setLevel(level)
    fh = logging.FileHandler('log\\' + logName + '.log', mode='w')
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
    logger.addHandler(fh)
    
def setDictValue(dict_, conceptID, value):
    if(dict_.get(conceptID, -1) == -1):
        dict_[conceptID] = value
    else:
        logging.getLogger('general').warning("Duplicated key " + str(conceptID) + " " +str(value))

def getValueFromElement(xmlElement, conceptID):
    value = None
    value0 = None
    #Valida que este dentro de las unidades permitidas (USD, SHARES, ETC)
    if(xmlElement.get('@unitRef', -1) != -1):
        if any(xmlElement['@unitRef'] in s for s in unitRefList):
            value0 = xmlElement.get('#text', -1)
        else:
            raise LoggingException('skipped', "unitRef not valid "  + conceptID + " " + str(xmlElement))
    else:
        raise LoggingException('notinclude_UnitRef', "Not unitRef " + conceptID + " " + str(xmlElement))
    try:
        if(value0 is not None):
            value = Decimal(value0)
            return value
    except InvalidOperation:
        raise LoggingException('InvalidOperation_convertToDecimal', "Error formating field " + str(xmlElement))

def addResultData(resultList, company, period, session):
    for cqr in resultList:
        try:
            concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(cqr.conceptID), session)
        except NoResultFound:
            logging.getLogger('general').debug("Add " +cqr.conceptID)
            concept = Concept()
            concept.conceptID = cqr.conceptID
            session.add(concept)
            session.flush()
        try:
            cqrToAdd = GenericDao.getOneResult(CompanyQResult, and_(CompanyQResult.company == company,  CompanyQResult.period == period, CompanyQResult.concept == concept, CompanyQResult.periodType == cqr.periodType), session)
        except NoResultFound:
            cqrToAdd = CompanyQResult()
        cqrToAdd.company = company
        cqrToAdd.concept = concept
        cqrToAdd.period = period
        cqrToAdd.value = cqr.value
        cqrToAdd.periodType = cqr.periodType
        session.add(cqrToAdd)
        logging.getLogger('addResultData').debug("Added " + str(cqr.conceptID) + " " + cqr.periodType)
    session.commit()
    
def getFiscalYear(documentPeriodYear, fiscalYear, yearStrategy):
    if(documentPeriodYear != fiscalYear):
        if (yearStrategy == YearStrategy.ERROR):
            raise Exception('FISCAL YEAR IS DIF THAN PERIOD YEAR ' + fiscalYear + " " + documentPeriodYear)
        else:
            logging.getLogger('general').warning('FISCAL YEAR IS DIF THAN PERIOD YEAR ' + fiscalYear + " " + documentPeriodYear)
            fiscalYearToUse = fiscalYear
    if(yearStrategy == YearStrategy.FISCAL_YEAR):
        fiscalYearToUse = fiscalYear
    elif(yearStrategy == YearStrategy.DOC_PERIOD_END_DATE):
        fiscalYearToUse = documentPeriodYear
    else:
        fiscalYearToUse = fiscalYear
    return fiscalYearToUse

def getCQRListFromTagName(xmlDictRoot, tagName, contextRefDict):
    resultList = []
    if isinstance(xmlDictRoot[tagName], list):
        for xmlElement in xmlDictRoot[tagName]:
            try:
                cqr =  getCQRFromElement(element = xmlElement, tagName = tagName, contextRefDict = contextRefDict)
                logging.getLogger('tempData').debug("Add to list " + cqr.conceptID)
                resultList.append(cqr)
            except LoggingException as le:
                le.log()
    else:
        try:
            cqr =  getCQRFromElement(element = xmlDictRoot[tagName], tagName = tagName, contextRefDict = contextRefDict)
            resultList.append(cqr)
        except LoggingException as le:
            le.log()
    if(not resultList):
        logging.getLogger('matchList_empty').debug(tagName + " " + str(xmlDictRoot[tagName]))
    return resultList


def getCQRFromElement(element, tagName, contextRefDict):
        contextRefToUseFI = contextRefDict.get("FIX", -1)
        contextRefToUseFDQTD = contextRefDict.get("QTD", -1)
        contextRefToUseFDYTD = contextRefDict.get("YTD", -1)
        conceptID = tagName[8:len(tagName)]
        #Si tiene context ref
        if(element.get('@contextRef', -1) != -1):
            contextRef = element['@contextRef']
            cqr = CompanyQResult()
            cqr.conceptID = conceptID
            cqr.value = getValueFromElement(element, conceptID)
            cqr.row = element
            #Si conextRef es igual a FI2018Q2
            if(contextRef == contextRefToUseFI):
                cqr.periodType = "FIX"
            #Si contextRef es igual a FD2018Q2QTD
            elif(contextRef == contextRefToUseFDQTD):
                cqr.periodType = "QTD"
            #Si contextRef es igual a FD2018Q2QTD
            elif(contextRef == contextRefToUseFDYTD):
                cqr.periodType = "YTD"
            else:
                raise LoggingException('skipped', "Skipped concept for " + contextRefToUseFI + " " + tagName + " " + str(element))
            return cqr
        else:
            raise LoggingException('notinclude_ContextRef', str(element))

def getCQRListFromXML(xmlDictRoot, contextRefDict):
    qrcResultList = []
    for tagName in xmlDictRoot:
        if(tagName[0:8] == "us-gaap:" and tagName.find("TextBlock") == -1):
            returnList = getCQRListFromTagName(xmlDictRoot, tagName, contextRefDict)
            if (returnList is not None):
                qrcResultList = qrcResultList + returnList
    return qrcResultList

def getFiscalYear2(xmlDictRoot):
    fiscalYear = getValueWithTagDict(tagNameAlias['DOCUMENT_FISCAL_YEAR_FOCUS'], xmlDictRoot)['#text']
    logging.getLogger('general').debug("fiscalYear " + fiscalYear)
    documentPeriodEndDate = getValueWithTagDict(tagNameAlias['DOCUMENT_PERIOD_END_DATE'], xmlDictRoot)['#text']
    logging.getLogger('general').debug("documentPeriodEndDate " + documentPeriodEndDate)
    documentPeriodYear = documentPeriodEndDate[0:4]
    return getFiscalYear(documentPeriodYear, fiscalYear, yearStrategy)

def getFiscalPeriod(xmlDictRoot):
    fiscalPeriod = getValueWithTagDict(tagNameAlias['DOCUMENT_FISCAL_PERIOD_FOCUS'], xmlDictRoot)['#text']
    fiscalPeriod = fiscalPeriod[1:2]
    logging.getLogger('general').debug("fiscalPeriod " + fiscalPeriod)
    if (fiscalPeriod =='Y'):
        fiscalPeriod = 4
    return fiscalPeriod
        
def getXmlDictFromText(fileText, tagKey, key, mainTag):
    xmlText = getXMLFromText(fileText, tagKey, key, mainTag)
    xmlDict = xmltodict.parse(xmlText)
    return xmlDict

def getXMLFromText(fileText, tagKey, key, mainTag):
    point1 = fileText.find("<" + tagKey + ">" + key, 0, len(fileText))
    point2 = fileText.find("<" + mainTag +">", point1, len(fileText)) + len("<" + mainTag + ">")+1
    point3 = fileText.find("</" + mainTag + ">", point2, len(fileText))
    xmlText = fileText[point2:point3]
    #logging.getLogger('bodyXML').debug(xmlText)
    return xmlText

def getContextRefDict(xmlDictRoot):
    contextRefDict = {}
    for item in getValueWithTagDict(tagNameAlias['XBRL_CONTEXT'], xmlDictRoot):
        id_ = item['@id']
        periodElement = getValueWithTagDict(tagNameAlias['XBRL_PERIOD'], item)
        startDate = getValueAsDate('XBRL_START_DATE', periodElement)
        endDate = getValueAsDate('XBRL_END_DATE', periodElement) 
        instant = getValueAsDate('XBRL_INSTANT', periodElement)
        entityElement = getValueWithTagDict(tagNameAlias['XBRL_ENTITY'], item)
        if(getValueWithTagDict(tagNameAlias['XBRL_SEGMENT'], entityElement, False) == -1):
            documentPeriodEndDate = getValueAsDate('DOCUMENT_PERIOD_END_DATE', xmlDictRoot)['#text']
            documentPeriodEndDate = datetime.strptime(documentPeriodEndDate, '%Y-%m-%d')
            #logging.getLogger('general').debug("item " + str(item))
            if(getDaysBetweenDates(instant, documentPeriodEndDate) < 5):
                contextRefDict["FIX"] = id_
            elif(getDaysBetweenDates(documentPeriodEndDate, endDate) < 5):
                #logging.getLogger('general').debug("item " + str(item))
                endDatePreviousYear = documentPeriodEndDate.replace(year = documentPeriodEndDate.year -1, month = 12, day = 31)
                if(getDaysBetweenDates(startDate, endDatePreviousYear) < 5):
                    contextRefDict["YTD"] = id_
                elif(85 < getDaysBetweenDates(startDate, documentPeriodEndDate) < 95):
                    contextRefDict["QTD"] = id_
    return contextRefDict

def getValueAsDate(tagConstant, xmlElement):
    value = getValueWithTagDict(tagNameAlias[tagConstant], xmlElement, False)
    if(isinstance(value, str)):
        return datetime.strptime(value, '%Y-%m-%d')
    else:
        return value
        
def getDaysBetweenDates(firstDate, secondDate):
    if(secondDate != -1 and firstDate != -1):
        return abs((secondDate - firstDate).days)
    else:
        return 10000
    

def importSECFile(filenameList, replace, yearStrategy, company, session):
        for filename in filenameList:
            logging.getLogger('general').debug("filename " + filename)
            fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
            #Obtengo reportes statements
            xmlDict= getXmlDictFromText(fileText, "FILENAME", "FilingSummary.xml", "XML")
            reportRoleDict = {}
            for report in xmlDict["FilingSummary"]["MyReports"]["Report"]:
                if(report.get("MenuCategory", -1) == "Statements"):
                    reportRoleDict[report["Role"]] = list()
                    logging.getLogger('bodyXML').debug(report)
            #Obtengo para cada reporte sus conceptos
            xmlDict2= getXmlDictFromText(fileText, "TYPE", "EX-101.PRE", "XBRL")
            for item in getValueWithTagDict(tagNameAlias['PRESENTATON_LINK'], getValueWithTagDict(tagNameAlias['LINKBASE'], xmlDict2)): 
                reportRole = item['@xlink:role']
                if any(reportRole in s for s in reportRoleDict.keys()):
                    #logging.getLogger('bodyXML').debug(item)
                    for item2 in getValueWithTagDict(tagNameAlias['LOC'], item):
                        fact = Fact()
                        fact.xlink_href = item2["@xlink:href"]
                        reportRoleDict[reportRole].append(fact)
                        #logging.getLogger('bodyXML').debug(item2["@xlink:href"])
            
            if (1 == 1):
                xsdDictCache = {}
                for reportName, factList in reportRoleDict.items():
                    for fact in factList:
                        xsdURL = fact.xlink_href[0: fact.xlink_href.find("#", 0)]
                        conceptID = fact.xlink_href[fact.xlink_href.find("#", 0) + 1:len(fact.xlink_href)]
                        #logging.getLogger('bodyXML').debug(xsdURL)
                        if(xsdURL[0:4] == "http"):
                            xsdFileName = xsdURL[xsdURL.rfind("/") + 1: len(xsdURL)]
                            #logging.getLogger('bodyXML').debug(xsdFileName)
                            if(isinstance(xsdDictCache.get(xsdFileName, -1), int)):
                                xsdFile = getXSDFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//xsd//" + xsdFileName, xsdURL)
                                xsdDict = xmltodict.parse(xsdFile)
                                xsdDF = pd.DataFrame(xsdDict["xs:schema"]["xs:element"])
                                xsdDF.set_index("@id", inplace=True)
                                xsdDF.head()
                                xsdDictCache[xsdFileName] = xsdDF
                            else:
                                xsdDF = xsdDictCache.get(xsdFileName, -1)
                            fact.conceptName = xsdDF.loc[conceptID]["@name"]
                            fact.periodType = xsdDF.loc[conceptID]["@xbrli:periodType"]
                            fact.balance = xsdDF.loc[conceptID]["@xbrli:balance"]
                            fact.type = xsdDF.loc[conceptID]["@type"]
                            fact.abstract = xsdDF.loc[conceptID]["@abstract"]
                            if(fact.abstract == "true"):
                                reportRoleDict[reportName].remove(fact)
                            logging.getLogger('bodyXML').debug(fact.__dict__)
                        else:
                            a = 1
                            #logging.getLogger('xsdNotFound').debug(fact.__dict__)
                
                for reportName, factList in reportRoleDict.items():
                    for fact in factList:
                        #if(fact.conceptName == "us-gaap_CashAndCashEquivalentsAtCarryingValue"):
                            logging.getLogger('bodyXML').debug(reportName + " " + str(fact.__dict__))       
                
                if (1==0):
                    xmlDictRoot = getXmlDictFromText(fileText, "EX-101.INS")
                    CIK = xmlDictRoot['dei:EntityCentralIndexKey']['#text']
                    logging.getLogger('general').debug("CIK " + CIK)
                    documentType = xmlDictRoot['dei:DocumentType']['#text']
                    fiscalYearToUse = getFiscalYear2(xmlDictRoot)
                    fiscalPeriod = getFiscalPeriod(xmlDictRoot)
                    logging.getLogger('general').debug("documentType " + documentType)
                    contextRefDict = getContextRefDict(xmlDictRoot)
                    logging.getLogger('general').debug("contextRefDict " + str(contextRefDict))
                    if(1== 1):
                        qrcResultList = getCQRListFromXML(xmlDictRoot, contextRefDict)
                        period = GenericDao.getOneResult(Period, and_(Period.year.__eq__(fiscalYearToUse),Period.quarter.__eq__(fiscalPeriod)), session)
                        if(replace or not session.query(exists().where(and_(CompanyQResult.period == period, CompanyQResult.company == company))).scalar()):
                            addResultData(qrcResultList, company, period, session)
                        else:
                            logging.warning("Exists result for period " + str(period.year) + "-" +  str(period.quarter))

def readSECIndexFor(period, company, replace, yearStrategy):
    logging.info("Processing index file " + str(period.year) + "-" +  str(period.quarter))   
    file = getBinaryFileFromCache('C://Users//afunes//iCloudDrive//PortfolioViewer//cache//master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/master.gz")
    if 1 == 1:
        with gzip.open(BytesIO(file), 'rb') as f:
            file_content = f.read()
            text = file_content.decode("utf-8")
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
                        importSECFile([filename], replace, yearStrategy, company, session)
            else:
                filename = rowData0["Filename"]
                formType = rowData0["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):
                    importSECFile([filename], replace, yearStrategy, company, session)

COMPANY_TICKER = 'AMD'
replace = True
yearStrategy = YearStrategy.DOC_PERIOD_END_DATE
Initializer()
session = DBConnector().getNewSession()
company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
#periodList = objectResult = session.query(Period).filter(and_(or_(Period.year < 2018, and_(Period.year >= 2018, Period.quarter <= 3)), Period.year > 2012)).order_by(Period.year.asc(), Period.quarter.asc()).all()
periodList = objectResult = session.query(Period).filter(and_(Period.year == 2018, Period.quarter == 2)).order_by(Period.year.asc(), Period.quarter.asc()).all()
createLog('general', logging.DEBUG)
createLog('bodyIndex', logging.INFO)
createLog('bodyXML', logging.DEBUG)
createLog('InvalidOperation_convertToDecimal', logging.DEBUG)
createLog('notinclude_UnitRef', logging.INFO)
createLog('notinclude_ContextRef', logging.INFO)
createLog('skipped_underscore', logging.INFO)
createLog('skipped', logging.INFO)
createLog('xsdNotFound', logging.DEBUG)
createLog('addResultData', logging.INFO)
createLog('matchList_empty', logging.INFO)
createLog('tempData', logging.INFO)

for period in periodList:
    readSECIndexFor(period, company, replace, yearStrategy)
    
