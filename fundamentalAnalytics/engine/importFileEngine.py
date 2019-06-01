'''
Created on May 25, 2019

@author: afunes
'''
from _io import BytesIO, StringIO
import concurrent.futures
from concurrent.futures.thread import ThreadPoolExecutor
import gzip
import logging
import os
from threading import Semaphore
import threading

import pandas
import requests

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
from tools.tools import getBinaryFileFromCache, getXMLFromText, \
    FileNotFoundException, getXMLDictFromGZCache, XSDNotFoundException, \
    XMLNotFoundException
from valueobject.constant import Constant
from valueobject.valueobject import ImportFileVO


class ImportFileEngine():
    
    @staticmethod
    def importMasterIndexFor(period, replaceMasterFile, session, threadNumber = 1):
        file = getBinaryFileFromCache(Constant.CACHE_FOLDER + 'master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                    "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/master.gz", replaceMasterFile)
        with gzip.open(BytesIO(file), 'rb') as f:
            file_content = f.read()
            text = file_content.decode("ISO-8859-1")
            text = text[text.find("CIK", 0, len(text)): len(text)]
            point1 = text.find("\n")
            point2 = text.find("\n", point1+1)
            text2 = text[0:point1] + text[point2 : len(text)]
            df = pandas.read_csv(StringIO(text2), sep="|")
            df.set_index("CIK", inplace=True)
            df.head()
            executor = ThreadPoolExecutor(max_workers=threadNumber)
            print("STARTED")
            for row in df.iterrows():
                filename = row[1]["Filename"]
                formType = row[1]["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):
                    importVO = ImportFileVO(filename)
                    executor.submit(importVO.importFile)
            print("FINISHED")
    
    @staticmethod
    def importFiles(filename, reimport = False):
        try:
            session = DBConnector().getNewSession()
            fileData = FileDataDao.getFileData(filename, session)
            if(fileData is None or (fileData.importStatus != "OK") or reimport):
                FileDataDao.addOrModifyFileData("PENDING", "INIT", filename, session)
                fullFileName = Constant.CACHE_FOLDER + filename
                fullFileName = fullFileName[0: fullFileName.find(".txt")]
                logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("START - Processing index file " + fullFileName)   
                if(ImportFileEngine.validateIfSomeFilesNotExits(fullFileName) or reimport):
                    url = "https://www.sec.gov/Archives/" + filename
                    logging.getLogger(Constant.LOGGER_GENERAL).debug("URL " + url)
                    response = requests.get(url, timeout = 30) 
                    fileText = response.text
                    if not os.path.exists(fullFileName): 
                        os.makedirs(fullFileName)
                    ImportFileEngine.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
                    try:
                        ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
                    except XMLNotFoundException:#TODO mejorar esto
                        summaryDict = getXMLDictFromGZCache(filename, Constant.DOCUMENT_SUMMARY)
                        for file in summaryDict["FilingSummary"]["InputFiles"]['File']:
                            print(file)
                            if isinstance(file, dict):
                                if(file["@doctype"] == "10-Q") or file["@doctype"] =="10-K":
                                    instFilename = file["#text"]
                                    instFilename = instFilename.replace(".", "_") + ".xml"
                                    ImportFileEngine.saveFile(fileText,"FILENAME", instFilename, "XML",fullFileName, hardKey=Constant.DOCUMENT_INS)
                                    break
                    ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
                    ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName) 
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - SUCCESSFULLY " + fullFileName)
                    FileDataDao.addOrModifyFileData("PENDING", "OK", filename, session)
                else:
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - EXISTS " + fullFileName)
                    FileDataDao.addOrModifyFileData("PENDING", "OK", filename, session)  
        except (FileNotFoundException, XSDNotFoundException, XMLNotFoundException) as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + str(e))
            FileDataDao.addOrModifyFileData("PENDING", e.importStatus, filename, errorMessage=str(e))
        except Exception as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + url)
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).exception(e)
            FileDataDao.addOrModifyFileData("PENDING", "IMP ERROR", filename, errorMessage=str(e))
        finally:
            session.remove()
            session.close()
    
    @staticmethod       
    def validateIfSomeFilesNotExits(folder):
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_INS + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_PRE + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SCH + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SUMMARY + ".gz"):
            return True
    
    @staticmethod         
    def saveFile(fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False, hardKey = None):
        if(hardKey is None):
            hardKey = key
        xmlFile= getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + hardKey + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
    
    @staticmethod        
    def saveFile2(fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                ImportFileEngine.saveFile(fileText, tagKey, key, mainTag, fullFileName)
                break
            except Exception as e:
                if(mainTagList[-1] == mainTag):
                    raise e
                else:
                    print(e)