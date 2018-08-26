'''
Created on 22 ago. 2018

@author: afunes
'''
from _decimal import Decimal
import logging

import requests
import xmltodict 

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.period import Period
from sqlalchemy.sql.expression import and_
from modelClass.companyqresult import CompanyQResult


def setDictValue(dict_, key, value):
    if(dict_.get(key, -1) == -1):
        dict_[key] = value
    else:
        logging.warning("Duplicated key " + str(key) + " " +str(value))
        
response = requests.get("https://www.sec.gov/Archives/edgar/data/50863/000005086318000022/0000050863-18-000022.txt", timeout = 10) 
print('Response status ' + str(response.status_code))
point1 = response.text.find("EX-101.INS", 0, len(response.text))
point2 = response.text.find("<XBRL>", point1, len(response.text)) + len("<XBRL>")+1
point3 = response.text.find("</XBRL>", point2, len(response.text))
xmlText = response.text[point2:point3]
xmlDict = xmltodict.parse(xmlText)
resultDict = {}
for key in xmlDict['xbrli:xbrl']:
    if(key[0:8] == "us-gaap:" and key.find("TextBlock") == -1):
        conceptID = key[8:len(key)]
        value = None
        if isinstance(xmlDict['xbrli:xbrl'][key], list):
            value0 = xmlDict['xbrli:xbrl'][key][0].get('#text', -1)
            if(value0 != -1 and value0.isdigit()):
                value = Decimal(value0)
                #print(conceptID + " " + str(value))
                setDictValue(resultDict, conceptID, value)
        else:
            value0 = (xmlDict['xbrli:xbrl'][key]['#text'])
            if value0.isdigit():
                value = Decimal(value0)
                #print(conceptID + " " + str(value))
                setDictValue(resultDict, conceptID, value)

CIK = xmlDict['xbrli:xbrl']['dei:EntityCentralIndexKey']['#text']
print(CIK)
fiscalYear = xmlDict['xbrli:xbrl']['dei:DocumentFiscalYearFocus']['#text']
print(fiscalYear)
fiscalPeriod = xmlDict['xbrli:xbrl']['dei:DocumentFiscalPeriodFocus']['#text']
fiscalPeriod = fiscalPeriod[1:2]
print(fiscalPeriod)
documentType = xmlDict['xbrli:xbrl']['dei:DocumentType']['#text']
print(documentType)

Initializer()
session = DBConnector().getNewSession()

company = GenericDao.getOneResult(Company,Company.ticker.__eq__("INTC") , session)
period = GenericDao.getOneResult(Period, and_(Period.year.__eq__(fiscalYear),Period.quarter.__eq__(fiscalPeriod)), session)

for key, value in resultDict.items():
    concept = GenericDao.getOneResult(Concept, Concept.conceptID.__eq__(key), session)
    if(concept is None):
        logging.warning("Concept not exists " + conceptID)
    cqr = CompanyQResult()
    cqr.company = company
    cqr.concept = concept
    cqr.period = period
    cqr.value = value
    session.add(cqr)
    print("Added " + key + " " + str(value))
session.commit()
