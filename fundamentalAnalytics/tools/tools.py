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

def getValueAsDate(attrID, element):
    value = getValueFromElement(attrID, element, False)
    if(value is not None):
        return datetime.strptime(value, '%Y-%m-%d')

def getDaysBetweenDates(firstDate, secondDate):
    if(secondDate is not None and firstDate is not None):
        return abs((secondDate - firstDate).days)
    else:
        return 10000

def getObjectFromElement(objectIDList, element):
    for objectID in objectIDList:
        if(element.get(objectID, None) is not None):
            return element.get(objectID)
        
def getObjectFromList(objectIDList, list_):
    for objectID in objectIDList:
        for itemList in list_:
            if(itemList.get(objectID, None) is not None):
                return itemList#TODO
            
def getListFromElement(elementIDList, element, raiseException = True):
    obj = getObjectFromElement(elementIDList, element)
    if (obj is None):
        if (raiseException):
            raise Exception("List for elementID not found "  + str(elementIDList) + " " +  str(element)[0:50])
    elif(not isinstance(obj, list)):
        raise Exception("List for elementID is not list "  + str(elementIDList) + " " +  str(element)[0:50])
    else:
        return obj

def getElementFromElement(elementIDList, element, raiseException = True):
    obj = getObjectFromElement(elementIDList, element)
    if (obj is None):
        if (raiseException):
            raise Exception("Element for elementID not found "  + str(elementIDList) + " " +  str(element)[0:50])
    elif(not isinstance(obj, dict)):
        raise Exception("Element for elementID is not dict "  + str(elementIDList) + " " +  str(element)[0:50])
    else:
        return obj

def getValueFromElement(attrIDList, element, raiseException = True):
    if(isinstance(element, dict)):
        obj = getObjectFromElement(attrIDList, element)
    elif(isinstance(element, list)):
        obj = getObjectFromList(attrIDList, element)
    if (obj is None):
        if (raiseException):
            raise Exception("Value for attrID not found "  + str(attrIDList) + " " +  str(element)[0:50])
    elif(isinstance(obj, dict)):
        return getValueFromElement(attrIDList, obj, raiseException)
    elif(not isinstance(obj, str)):
        raise Exception("Value for elementID is not str "  + str(attrIDList) + " " +  str(element)[0:50])
    else:
        return obj

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
        

