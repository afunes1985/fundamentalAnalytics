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


from enum import Enum
class YearStrategy(Enum):
    FISCAL_YEAR = 1
    DOC_PERIOD_END_DATE = 2
    ERROR = 3
    
unitRefList = ['usd', 'number', 'shares', 'usdPerShare']

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

def getValueFromKey(xmlDictRoot, key, contextRefToUse):
    try:
        value0 = None
        value = None
        xmlItem = None
        contextRefToUseFI = "FI" + contextRefToUse
        contextRefToUseFD = "FD" + contextRefToUse
        if isinstance(xmlDictRoot[key], list):
            logging.getLogger('info').debug("***************************** " + str(xmlDictRoot[key]))
            for item in xmlDictRoot[key]:
                if(item.get('@contextRef', -1) != -1):
                    if(item.get('@contextRef', -1) == contextRefToUseFI):
                        logging.getLogger('info').info(str(item.get('@contextRef', -1)) + " " + str(item.get('#text', -1)))
                        xmlItem = item
                    elif(len(item.get('@contextRef', -1)) >= 11):
                        logging.getLogger('info').info(str(item.get('@contextRef', -1)) + " " + str(item.get('#text', -1)))
                        xmlItem = item
                    else:
                        if(item.get('@contextRef', -1)[0:2] == "FI"):
                            logging.getLogger('notFound_FI').debug("Not Found for " + contextRefToUseFI + " " + key + " " + str(item))
                        elif(item.get('@contextRef', -1)[0:2] == "FD"):
                            logging.getLogger('notFound_FD').debug("Not Found for " + contextRefToUseFD + " " + key + " " + str(item))
                        else:
                            logging.getLogger('skippedConcept').debug("Skipped concept for " + contextRefToUseFI + " " + str(item))
                        return None
                else:
                    logging.getLogger('notContextRef').debug(item)
        else:
            xmlItem = xmlDictRoot[key]
            
        if(xmlItem.get('@unitRef', -1) != -1):
            if any(xmlItem['@unitRef'] in s for s in unitRefList):
                value0 = xmlItem.get('#text', -1)
            else:
                logging.getLogger('skippedConcept').info("Skipped concept " + str(xmlItem))
                return None
        else:
            logging.getLogger('notUnitRef').debug("Not unitRef " + str(xmlItem))
            return None
        try:
            if(value0 is not None):
                value = Decimal(value0)
                cqr = CompanyQResult()
                cqr.value = value
                return cqr
        except InvalidOperation:
            logging.getLogger('InvalidOperation').warning("Error formating field " + str(xmlItem))
    except Exception:
        logging.getLogger('general').exception("Error formating field " + key)
        return None

def addResultData(resultDict, company, period, session):
    for conceptID, cqr in resultDict.items():
        try:
            concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(conceptID), session)
        except NoResultFound:
            logging.getLogger('general').debug("Add " +conceptID)
            concept = Concept()
            concept.conceptID = conceptID
            session.add(concept)
            session.flush()
        try:
            cqrToAdd = GenericDao.getOneResult(CompanyQResult, and_(CompanyQResult.company == company,  CompanyQResult.period == period, CompanyQResult.concept == concept), session)
        except NoResultFound:
            cqrToAdd = CompanyQResult()
        cqrToAdd.company = company
        cqrToAdd.concept = concept
        cqrToAdd.period = period
        cqrToAdd.value = cqr.value
        cqrToAdd.periodType = cqr.periodType
        session.add(cqrToAdd)
        logging.getLogger('addResultData').debug("Added " + str(cqrToAdd.__dict__))
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

def importSECFile(filenameList, replace, yearStrategy, session):
        for filename in filenameList:
            logging.info("Processing file " + filename)
            fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
            point1 = fileText.find("EX-101.INS", 0, len(fileText))
            point2 = fileText.find("<XBRL>", point1, len(fileText)) + len("<XBRL>")+1
            point3 = fileText.find("</XBRL>", point2, len(fileText))
            xmlText = fileText[point2:point3]
            xmlDict = xmltodict.parse(xmlText)
            #logging.debug(xmlText)
            resultDict = {}
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
            for key in xmlDictRoot:
                if(key[0:8] == "us-gaap:" and key.find("TextBlock") == -1):
                    conceptID = key[8:len(key)]
                    cqr = getValueFromKey(xmlDictRoot, key, contextRefToUse)
                    if cqr is not None:
                        setDictValue(resultDict, conceptID, cqr)
            company = GenericDao.getOneResult(Company,Company.ticker.__eq__("INTC") , session)
            period = GenericDao.getOneResult(Period, and_(Period.year.__eq__(fiscalYearToUse),Period.quarter.__eq__(fiscalPeriod)), session)
            
            if(replace or not session.query(exists().where(and_(CompanyQResult.period == period, CompanyQResult.company == company))).scalar()):
                addResultData(resultDict, company, period, session)
            else:
                logging.warning("Exists result for period " + str(period.year) + "-" +  str(period.quarter))

def getBinaryFileFromCache(filename, url):
    xbrlFile = Path(filename)
    if xbrlFile.exists():
        with open(filename, mode='rb') as file: # b is important -> binary
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
            rowData0 = df.loc[company.CIK]
            if isinstance(rowData0, DataFrame):
                for rowData1 in rowData0.iterrows():
                    filename = rowData1[1]["Filename"]
                    formType = rowData1[1]["Form Type"]
                    if(formType == "10-Q" or formType == "10-K"):
                        importSECFile([filename], replace, yearStrategy, session)
            else:
                filename = rowData0["Filename"]
                formType = rowData0["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):
                    importSECFile([filename], replace, yearStrategy, session)

COMPANY_TICKER = 'INTC'
replace = True
yearStrategy = YearStrategy.DOC_PERIOD_END_DATE
Initializer()
session = DBConnector().getNewSession()
company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
periodList = objectResult = session.query(Period).filter(and_(or_(Period.year < 2018, and_(Period.year >= 2018, Period.quarter <= 3)), Period.year > 2016)).order_by(Period.year.asc(), Period.quarter.asc()).all()

createLog('InvalidOperation', logging.DEBUG)
createLog('general', logging.DEBUG)
createLog('info', logging.INFO)
createLog('skippedConcept', logging.DEBUG)
createLog('notUnitRef', logging.INFO)
createLog('notContextRef', logging.INFO)
createLog('notFound_FD', logging.DEBUG)
createLog('notFound_FI', logging.DEBUG)
createLog('addResultData', logging.INFO)

for period in periodList:
    readSECIndexFor(period, company, replace, yearStrategy)
    
