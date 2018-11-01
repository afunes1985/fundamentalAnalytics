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

from base.dbConnector import DBConnector
from dao.dao import GenericDao, Dao
from modelClass.fileData import FileData
from modelClass.period import Period
from valueobject.constant import Constant
from valueobject.valueobject import FactVO, FactValueVO


def getBinaryFileFromCache(filename, url = None):
    #logging.getLogger(Constant.LOGGER_GENERAL).debug("BIN - Processing filename " + filename.replace("//", "/"))
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
    logging.getLogger(Constant.LOGGER_GENERAL).debug("TXT - Processing filename " + filename.replace("//","/"))
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
    logging.getLogger(Constant.LOGGER_GENERAL).debug("XSD - Processing filename " + filename)
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

def getDaysBetweenDates(firstDate, secondDate):
    if(secondDate is not None and firstDate is not None):
        return abs((secondDate - firstDate).days)
    else:
        return 10000

def getXmlDictFromText(fileText, tagKey, key, mainTag):
    xmlText = getXMLFromText(fileText, tagKey, key, mainTag)
    xmlDict = xmltodict.parse(xmlText)
    return xmlDict

def getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists = False):
    point1 = fileText.find("<" + tagKey + ">" + key, 0, len(fileText))
    if(point1 == -1 and skipIfNotExists == False):
        raise FileNotFoundException("Key " + key + " not found")
    elif(point1 == -1):
        return None
    point2 = fileText.find("<" + mainTag +">", point1, len(fileText)) + len("<" + mainTag + ">")+1
    if (0 < point2 - point1 < 150 ):
        point3 = fileText.find("</" + mainTag + ">", point2, len(fileText))
        xmlText = fileText[point2:point3]
        return xmlText
    else:
        return None

def setDictValue(dict_, conceptID, value):
    if(dict_.get(conceptID, -1) == -1):
        dict_[conceptID] = value
    else:   
        logging.getLogger(Constant.LOGGER_GENERAL).warning("Duplicated key " + str(conceptID) + " " +str(value))
        
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
        
class FileNotFoundException(Exception):
    def __init__(self, fileName):
        self.fileName = fileName
        
def addOrModifyFileData(status, importStatus = None, filename = None, externalSession = None, errorMessage = None):
    if (externalSession is None):
        session = DBConnector().getNewSession()
    else:
        session = externalSession
    fileData = Dao.getFileData(filename, session)
    if (fileData is None):
        fileData = FileData()
        fileData.fileName = filename
    fileData.status = status
    fileData.errorMessage = errorMessage
    if importStatus is not None:
        fileData.importStatus = importStatus
    Dao.addObject(objectToAdd = fileData, session = session, doCommit = True)
    if (externalSession is None):
        session.close()
    return fileData

