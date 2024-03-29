'''
Created on 8 sep. 2018

@author: afunes
'''
from _io import BytesIO
import gzip
import logging
import os
from pathlib import Path

import dash
import requests
import xmltodict

from valueobject.constant import Constant


def getButtonID():
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    return button_id

def showQuery(query, dbconnector):
    print (query.statement.compile(dbconnector.engine))

def getBinaryFileFromCache(filename, url = None, replaceMasterFile = False):
    #logging.getLogger(Constant.LOGGER_GENERAL).debug("BIN - Processing filename " + filename.replace("//", "/"))
    xbrlFile = Path(filename)
    file = None
    if xbrlFile.exists() and not replaceMasterFile:
        with open(filename, mode='rb') as file: 
            file = file.read()
    elif (url is not None):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
        response = requests.get(url, timeout=30, headers=headers) 
        if(response.status_code == 200):
            directory = os.path.dirname(filename)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(filename, 'wb') as f:
                f.write(response.content)
                file = response.content
        else:
            print(response.content.decode())
            raise Exception(response.content.decode())
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
    return logger
    

class LoggingException(Exception):
    def __init__(self, loggerName, message):
        self.loggerName = loggerName
        self.message = message

    def log(self):
        logging.getLogger(self.loggerName).debug(self.message)
 
class CustomException(Exception):
    extraData = None
    message = None
    def __init__(self, message=None):
        if (message is not None):
            super().__init__(message)
            self.message=message
        else:
            super().__init__(self.status)
            self.message=self.status

class FileNotFoundException(CustomException):
    status = "FNF"

class XSDNotFoundException(CustomException):
    status = Constant.STATUS_XSD_FNF
        
class XMLNotFoundException(CustomException):
    status = Constant.FILE_STATUS_XML_FNF
        
class PriceNotFoundException(CustomException):
    status = Constant.STATUS_PRICE_NOT_FOUND
    message = 'Price not found'
    def __init__(self, extraData):
        self.extraData=extraData  
    
class EntityFactNotFoundException(CustomException):
    status = Constant.STATUS_ERROR
    def __init__(self, message):
        super().__init__(message)
    
class LastPriceNotFound(CustomException):
    status = Constant.STATUS_LAST_PRICE_NF
    
class LastPriceIsTooOld(CustomException):
    status = Constant.STATUS_LAST_PRICE_IS_TOO_OLD
    
class ConceptValueHasMoreThanOneRow(CustomException):
    status = Constant.STATUS_ERROR
    message = 'Concept Value has more than one row'
    def __init__(self, extraData):
        self.extraData=extraData[0:149] 
           
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
        shortFileName = finalFileName[finalFileName.rfind("/") + 1: len(finalFileName)]
        raise FileNotFoundException(shortFileName)
        

def getNumberValueAsString(value):
    if(value > 1000000):
        value = round(value)
    if(value % 1):
        return round(value,4)
    else:
        intLen = len(str(int(value)))
        if(intLen < 4):
            return value
        elif(intLen < 7):
            return str(int(value/1000)) + " m" 
        elif(intLen >= 7):
            return str(int(value/1000000)) + " M" 
        # elif(intLen < 13):
        #    return str(int(value/1000000000)) + " B"
        else:
            value
            
def convertListToDDDict(resultList, indexLabel=0, indexValue=0):
    ddDict = []
    for row in resultList:
            ddDict.append({'label': row[indexLabel], 'value': row[indexValue]})  
    return ddDict

