'''
Created on 22 ago. 2018

@author: afunes
'''
from _decimal import Decimal, InvalidOperation
from _io import BytesIO, StringIO
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


class LoggingException(Exception):
    def __init__(self, loggerName, message):
        self.loggerName = loggerName
        self.message = message

    def log(self):
        logging.getLogger(self.loggerName).debug(self.message)

from enum import Enum
class YearStrategy(Enum):
    FISCAL_YEAR = 1
    DOC_PERIOD_END_DATE = 2
    ERROR = 3
    
unitRefList = ['usd', 'number', 'shares', 'usdPerShare']

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

def getValueFromElement(xmlElement):
    value = None
    value0 = None
    #Valida que este dentro de las unidades permitidas (USD, SHARES, ETC)
    if(xmlElement.get('@unitRef', -1) != -1):
        if any(xmlElement['@unitRef'] in s for s in unitRefList):
            value0 = xmlElement.get('#text', -1)
        else:
            raise LoggingException('skipped', "unitRef not valid " + str(xmlElement))
    else:
        raise LoggingException('notinclude_UnitRef', "Not unitRef " + str(xmlElement))
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

def getCQRListFromTagName(xmlDictRoot, tagName, contextRefToUse):
    resultList = []
    if isinstance(xmlDictRoot[tagName], list):
        for xmlElement in xmlDictRoot[tagName]:
            try:
                cqr =  getCQRFromElement(element = xmlElement, tagName = tagName, contextRefToUse = contextRefToUse)
                logging.getLogger('tempData').debug("Add to list " + cqr.conceptID)
                resultList.append(cqr)
            except LoggingException as le:
                le.log()
    else:
        try:
            cqr =  getCQRFromElement(element = xmlDictRoot[tagName], tagName = tagName, contextRefToUse = contextRefToUse)
            resultList.append(cqr)
        except LoggingException as le:
            le.log()
    if(not resultList):
        logging.getLogger('matchList_empty').debug(tagName + " " + contextRefToUse + " " + str(xmlDictRoot[tagName]))
    return resultList


def getCQRFromElement(element, tagName, contextRefToUse):
        contextRefToUseFI = "FI" + contextRefToUse
        contextRefToUseFDQTD = "FD" + contextRefToUse + "QTD"
        contextRefToUseFDYTD = "FD" + contextRefToUse + "YTD"
        conceptID = tagName[8:len(tagName)]
        #Si tiene context ref
        if(element.get('@contextRef', -1) != -1):
            contextRef = element['@contextRef']
            cqr = CompanyQResult()
            cqr.conceptID = conceptID
            cqr.value = getValueFromElement(element)
            cqr.row = element
            if(contextRef.find("_", 0) != -1):
                raise LoggingException('skipped_underscore', "With underscore " + contextRefToUseFI + " " + tagName + " " + str(element))
            #Si conextRef es igual a FI2018Q2
            elif(contextRef == contextRefToUseFI):
                cqr.periodType = "FIX"
                return cqr
            #Si contextRef es igual a FD2018Q2QTD
            elif(contextRef[0:11]  == contextRefToUseFDQTD):
                cqr.periodType = contextRef[8:11]
                return cqr
            #Si contextRef es igual a FD2018Q2QTD
            elif(contextRef[0:11]  == contextRefToUseFDYTD):
                cqr.periodType = contextRef[8:11]
                return cqr
            else:
                if(contextRef[0:2] == "FI"):
                    raise LoggingException('skipped_FI', "Not Found for " + contextRefToUseFI + " " + tagName + " " + str(element))
                elif(contextRef[0:2] == "FD"):
                    raise LoggingException('skipped_FD', "Not Found for " + contextRefToUseFDQTD + " " + tagName + " " + str(element))
                else:
                    raise LoggingException('skipped', "Skipped concept for " + contextRefToUseFI + " " + tagName + " " + str(element))
        else:
            raise LoggingException('notinclude_ContextRef', str(element))

def getCQRListFromXML(xmlDictRoot, contextRefToUse):
    qrcResultList = []
    for tagName in xmlDictRoot:
        if(tagName[0:8] == "us-gaap:" and tagName.find("TextBlock") == -1):
            returnList = getCQRListFromTagName(xmlDictRoot, tagName, contextRefToUse)
            if (returnList is not None):
                qrcResultList = qrcResultList + returnList
    return qrcResultList

def importSECFile(filenameList, replace, yearStrategy, company, session):
        for filename in filenameList:
            fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
            point1 = fileText.find("EX-101.INS", 0, len(fileText))
            point2 = fileText.find("<XBRL>", point1, len(fileText)) + len("<XBRL>")+1
            point3 = fileText.find("</XBRL>", point2, len(fileText))
            xmlText = fileText[point2:point3]
            xmlDict = xmltodict.parse(xmlText)
            logging.getLogger('bodyXML').debug(xmlText)
            
            xmlDictRoot = xmlDict.get('xbrli:xbrl', -1)
            if(xmlDictRoot == -1):
                xmlDictRoot = xmlDict.get('xbrl', -1)
            logging.getLogger('general').debug("filename " + filename)
            CIK = xmlDictRoot['dei:EntityCentralIndexKey']['#text']
            logging.getLogger('general').debug("CIK " + CIK)
            fiscalYear = xmlDictRoot['dei:DocumentFiscalYearFocus']['#text']
            logging.getLogger('general').debug("fiscalYear " + fiscalYear)
            fiscalPeriod = xmlDictRoot['dei:DocumentFiscalPeriodFocus']['#text']
            fiscalPeriod = fiscalPeriod[1:2]
            logging.getLogger('general').debug("fiscalPeriod " + fiscalPeriod)
            documentType = xmlDictRoot['dei:DocumentType']['#text']
            logging.getLogger('general').debug("documentType " + documentType)
            documentPeriodEndDate = xmlDictRoot['dei:DocumentPeriodEndDate']['#text']
            logging.getLogger('general').debug("documentPeriodEndDate " + documentPeriodEndDate)
            documentPeriodYear = documentPeriodEndDate[0:4]
            fiscalYearToUse = getFiscalYear(documentPeriodYear, fiscalYear, yearStrategy)
            if (fiscalPeriod =='Y'):
                fiscalPeriod = 4
            contextRefToUse = fiscalYearToUse + "Q" + str(fiscalPeriod)
            qrcResultList = getCQRListFromXML(xmlDictRoot, contextRefToUse)
            period = GenericDao.getOneResult(Period, and_(Period.year.__eq__(fiscalYearToUse),Period.quarter.__eq__(fiscalPeriod)), session)
            
            if(replace or not session.query(exists().where(and_(CompanyQResult.period == period, CompanyQResult.company == company))).scalar()):
                addResultData(qrcResultList, company, period, session)
            else:
                logging.warning("Exists result for period " + str(period.year) + "-" +  str(period.quarter))


def readSECIndexFor(period, company, replace, yearStrategy):
    logging.info("Processing index file " + str(period.year) + "-" +  str(period.quarter))   
    file = getBinaryFileFromCache('C://Users//afunes//iCloudDrive//PortfolioViewer//cache//xbrl' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/xbrl.gz")
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
            logging.getLogger('bodyIndex').debug(df.to_string())
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

COMPANY_TICKER = 'INTC'
replace = True
yearStrategy = YearStrategy.DOC_PERIOD_END_DATE
Initializer()
session = DBConnector().getNewSession()
company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
periodList = objectResult = session.query(Period).filter(and_(or_(Period.year < 2018, and_(Period.year >= 2018, Period.quarter <= 3)), Period.year > 2012)).order_by(Period.year.asc(), Period.quarter.asc()).all()
#periodList = objectResult = session.query(Period).filter(and_(Period.year == 2018, Period.quarter == 2)).order_by(Period.year.asc(), Period.quarter.asc()).all()
createLog('general', logging.DEBUG)
createLog('bodyIndex', logging.INFO)
createLog('bodyXML', logging.INFO)
createLog('InvalidOperation_convertToDecimal', logging.DEBUG)
createLog('notinclude_UnitRef', logging.INFO)
createLog('notinclude_ContextRef', logging.INFO)
createLog('skipped_FD', logging.INFO)
createLog('skipped_FI', logging.INFO)
createLog('skipped_underscore', logging.INFO)
createLog('skipped', logging.DEBUG)
createLog('addResultData', logging.INFO)
createLog('matchList_empty', logging.INFO)
createLog('tempData', logging.INFO)

for period in periodList:
    readSECIndexFor(period, company, replace, yearStrategy)
    
