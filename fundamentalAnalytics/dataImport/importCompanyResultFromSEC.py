'''
Created on 22 ago. 2018

@author: afunes
'''
from _decimal import Decimal
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


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

def setDictValue(dict_, conceptID, value):
    #logging.debug(conceptID + " " + str(value))
    if(dict_.get(conceptID, -1) == -1):
        dict_[conceptID] = value
    else:
        logging.warning("Duplicated key " + str(conceptID) + " " +str(value))

def getValueFromKey(xmlDictRoot, key):
    try:
        if isinstance(xmlDictRoot[key], list):
            value0 = xmlDictRoot[key][len(xmlDictRoot[key])-1].get('#text', -1)
        else:
            value0 = (xmlDictRoot[key]['#text'])
        if(value0 != -1 and value0.isdigit()):
            value = Decimal(value0)
            return value
        else:
            logging.warning("Error formating field " + key)
    except Exception as e:
        logging.critical(e) 

def importSECFile(filenameList, session):
        for filename in filenameList:
            logging.info("Processing file " + filename)
            #response = requests.get("https://www.sec.gov/Archives/edgar/data/50863/000005086318000022/0000050863-18-000022.txt", timeout = 10) 
            fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
            point1 = fileText.find("EX-101.INS", 0, len(fileText))
            point2 = fileText.find("<XBRL>", point1, len(fileText)) + len("<XBRL>")+1
            point3 = fileText.find("</XBRL>", point2, len(fileText))
            xmlText = fileText[point2:point3]
            xmlDict = xmltodict.parse(xmlText)
            logging.debug(xmlText)
            resultDict = {}
            xmlDictRoot = xmlDict.get('xbrli:xbrl', -1)
            if(xmlDictRoot == -1):
                xmlDictRoot = xmlDict.get('xbrl', -1)
            for key in xmlDictRoot:
                if(key[0:8] == "us-gaap:" and key.find("TextBlock") == -1):
                    conceptID = key[8:len(key)]
                    value = getValueFromKey(xmlDictRoot, key)
                    setDictValue(resultDict, conceptID, value)
            CIK = xmlDictRoot['dei:EntityCentralIndexKey']['#text']
            logging.debug("CIK " + CIK)
            fiscalYear = xmlDictRoot['dei:DocumentFiscalYearFocus']['#text']
            logging.debug("fiscalYear " + fiscalYear)
            fiscalPeriod = xmlDictRoot['dei:DocumentFiscalPeriodFocus']['#text']
            fiscalPeriod = fiscalPeriod[1:2]
            logging.debug("fiscalPeriod " + fiscalPeriod)
            documentType = xmlDictRoot['dei:DocumentType']['#text']
            logging.debug("documentType " + documentType)
            
            if (fiscalPeriod =='Y'):
                fiscalPeriod = 4
            company = GenericDao.getOneResult(Company,Company.ticker.__eq__("INTC") , session)
            period = GenericDao.getOneResult(Period, and_(Period.year.__eq__(fiscalYear),Period.quarter.__eq__(fiscalPeriod)), session)
            
            if(not session.query(exists().where(
                CompanyQResult.period == period)).scalar()):
                for conceptID, value in resultDict.items():
                    try:
                        #logging.debug("Get " +conceptID)
                        concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(conceptID), session)
                    except NoResultFound:
                        #logging.debug("Add " +conceptID)
                        concept = Concept()
                        concept.conceptID = conceptID
                        session.add(concept)
                        session.flush()
                    cqr = CompanyQResult()
                    cqr.company = company
                    cqr.concept = concept
                    cqr.period = period
                    cqr.value = value
                    session.add(cqr)
                    #logging.debug("Added " + conceptID + " " + str(value))
                session.commit()
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

def readSECIndexFor(period, company):
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
                    if(formType == "10-Q"):
                        importSECFile([filename], session)
                    elif(formType == "10-K"): 
                        importSECFile([filename], session)
            else:
                filename = rowData0["Filename"]
                formType = rowData0["Form Type"]
                if(formType == "10-Q"):
                    importSECFile([filename], session)
                elif(formType == "10-K"): 
                        importSECFile([filename], session)

COMPANY_TICKER = 'TSLA'
Initializer()
session = DBConnector().getNewSession()
company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER) , session)
periodList = objectResult = session.query(Period).filter(and_(or_(Period.year < 2018, and_(Period.year >= 2018, Period.quarter <= 3)), Period.year > 2011)).order_by(Period.year.asc(), Period.quarter.asc()).all()

for period in periodList:
    readSECIndexFor(period, company)
    
