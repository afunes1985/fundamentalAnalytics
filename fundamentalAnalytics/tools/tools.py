'''
Created on 8 sep. 2018

@author: afunes
'''
from _io import BytesIO
import gzip
import logging
import os
from pathlib import Path

import requests
import xmltodict

from valueobject import constant
from valueobject.constant import Constant


def getBinaryFileFromCache(filename, url = None, replaceMasterFile = False):
    #logging.getLogger(Constant.LOGGER_GENERAL).debug("BIN - Processing filename " + filename.replace("//", "/"))
    xbrlFile = Path(filename)
    file = None
    if xbrlFile.exists() and not replaceMasterFile:
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
        raise XMLNotFoundException("XML not found tagkey: " + tagKey + " key: " + key)
    elif(point1 == -1):
        raise XMLNotFoundException("XML not found tagkey: " + tagKey + " key: " + key)
    point2 = fileText.find("<" + mainTag +">", point1, len(fileText)) + len("<" + mainTag + ">")+1
    if (0 < point2 - point1 < 150 ):
        point3 = fileText.find("</" + mainTag + ">", point2, len(fileText))
        xmlText = fileText[point2:point3]
        return xmlText
    else:
        raise XMLNotFoundException("XML not found tagkey: " + tagKey + " key: " + key)

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
    importStatus = "FNF"
    status = "FNF"
    def __init__(self, fileName):
        self.fileName = fileName

class XSDNotFoundException(Exception):
    importStatus = Constant.STATUS_XSD_FNF
    status = Constant.STATUS_XSD_FNF
    def __init__(self, fileName):
        self.fileName = fileName
        
class XMLNotFoundException(Exception):
    importStatus = Constant.FILE_STATUS_XML_FNF
    def __init__(self, fileName):
        self.fileName = fileName
           
def getXMLDictFromGZCache(filename, documentName):
    finalFileName = Constant.CACHE_FOLDER + filename[0: filename.find(".txt")] + "/" + documentName + ".gz"
    logging.getLogger(Constant.LOGGER_GENERAL).debug("XML - Processing filename " + finalFileName.replace("//", "/"))
    file = getBinaryFileFromCache(finalFileName)
    if (file is not None):
        with gzip.open(BytesIO(file), 'rb') as f:
            file_content = f.read()
            text = file_content.decode("ISO-8859-1")
            xmlDict = xmltodict.parse(text)
            return xmlDict
    else:
        raise FileNotFoundException("File not found " + finalFileName.replace("//", "/"))
        


