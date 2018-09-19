'''
Created on 8 sep. 2018

@author: afunes
'''
from _datetime import datetime
from _io import BytesIO
import gzip
import logging
import os
from pathlib import Path

import pandas
import requests
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_
import xmltodict

from dao.dao import GenericDao, Dao
from modelClass.period import Period
from valueobject.constant import Constant
from valueobject.valueobject import FactVO, FactValueVO


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
                 "LOC" : ["link:loc", "loc"],
                 "SCHEMA" : ["xsd:schema", "schema"],
                 "ELEMENT" : ["xsd:element","element"],
                 "UNIT" : ["xbrli:unit","unit"],
                 "MEASURE" : ["xbrli:measure","measure"],
                 "PRESENTATIONARC" : ["link:presentationArc", "presentationArc"]
                }

def getBinaryFileFromCache(filename, url = None):
    logging.getLogger('general').debug("BIN - Processing filename " + filename.replace("//", "/"))
    xbrlFile = Path(filename)
    file = None
    if xbrlFile.exists():
        with open(filename, mode='rb') as file: 
            file = file.read()
    elif (url is not None):
        response = requests.get(url, timeout = 30) 
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filename, 'wb') as f:
            f.write(response.content)
            file = response.content
    return file

def getTxtFileFromCache(filename, url):
    logging.getLogger('general').debug("TXT - Processing filename " + filename.replace("//","/"))
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
    logging.getLogger('general').debug("XSD - Processing filename " + filename)
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
    
def getValueWithTagDict(tagnameList, element, raiseException = True):
    for tagname in tagnameList:
        if(element.get(tagname, -1) != -1):
            return element.get(tagname, -1)
    if (raiseException):
        raise Exception("Element for tagname not found "  + str(tagnameList) + " " +  str(element))
    else:
        return -1
def getXmlDictFromText2(fileText, tagKey, key, mainTagList):
    for mainTag in mainTagList:
        try:
            return getXmlDictFromText(fileText, tagKey, key, mainTag)
        except Exception as e:
            print(e)

def getXmlDictFromText(fileText, tagKey, key, mainTag):
    xmlText = getXMLFromText(fileText, tagKey, key, mainTag)
    xmlDict = xmltodict.parse(xmlText)
    return xmlDict

def getXMLFromText(fileText, tagKey, key, mainTag):
    point1 = fileText.find("<" + tagKey + ">" + key, 0, len(fileText))
    point2 = fileText.find("<" + mainTag +">", point1, len(fileText)) + len("<" + mainTag + ">")+1
    point3 = fileText.find("</" + mainTag + ">", point2, len(fileText))
    xmlText = fileText[point2:point3]
    return xmlText

def setDictValue(dict_, conceptID, value):
    if(dict_.get(conceptID, -1) == -1):
        dict_[conceptID] = value
    else:
        logging.getLogger('general').warning("Duplicated key " + str(conceptID) + " " +str(value))
        
def createLog(logName, level):
    logger= logging.getLogger(logName)
    logger.setLevel(level)
    fh = logging.FileHandler('log\\' + logName + '.log', mode='w')
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
    logger.addHandler(fh)
    

class LoggingException(Exception):
    def __init__(self, loggerName, message):
        self.loggerName = loggerName
        self.message = message

    def log(self):
        logging.getLogger(self.loggerName).debug(self.message)
        

