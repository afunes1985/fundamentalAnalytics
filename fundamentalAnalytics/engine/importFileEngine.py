'''
Created on May 25, 2019

@author: afunes
'''
from _io import BytesIO, StringIO
from concurrent.futures.thread import ThreadPoolExecutor
import gzip
import logging
import os

import pandas
import requests

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
from tools.tools import getBinaryFileFromCache, getXMLFromText, \
    FileNotFoundException, getXMLDictFromGZCache, XSDNotFoundException, \
    XMLNotFoundException
from valueobject.constant import Constant


class ImportFileEngine():
    
    def __init__(self, fileName):
        self.fileName = fileName
        self.replace = False
    
    def importMasterIndexFor(self, period, replaceMasterFile, session, threadNumber = 1):
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
                #if(formType == "10-Q" or formType == "10-K"):
                    #  importVO = ImportFileVO(filename)#FIXME
                    #  executor.submit(importVO.importFile)
            print("FINISHED")
    
    def doImport(self):
        try:
            session = DBConnector().getNewSession()
            fileData = FileDataDao().getFileData(self.fileName, session)
            if(fileData is None or (fileData.importStatus != Constant.STATUS_OK) or self.replace):
                FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = Constant.STATUS_INIT, filename = self.fileName, externalSession = session)
                fullFileName = Constant.CACHE_FOLDER + self.fileName
                fullFileName = fullFileName[0: fullFileName.find(".txt")]
                logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("START - Processing index file " + fullFileName)   
                if(self.validateIfSomeFilesNotExits(fullFileName) or self.replace):
                    url = "https://www.sec.gov/Archives/" +self.fileName
                    logging.getLogger(Constant.LOGGER_GENERAL).debug("URL " + url)
                    response = requests.get(url, timeout = 30) 
                    fileText = response.text
                    if not os.path.exists(fullFileName): 
                        os.makedirs(fullFileName)
                    self.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
                    try:
                        self.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
                    except XMLNotFoundException:#TODO mejorar esto
                        summaryDict = getXMLDictFromGZCache(self.fileName, Constant.DOCUMENT_SUMMARY)
                        for file in summaryDict["FilingSummary"]["InputFiles"]['File']:
                            print(file)
                            if isinstance(file, dict):
                                if(file["@doctype"] == "10-Q") or file["@doctype"] =="10-K":
                                    instFilename = file["#text"]
                                    instFilename = instFilename.replace(".", "_") + ".xml"
                                    self.saveFile(fileText,"FILENAME", instFilename, "XML",fullFileName, hardKey=Constant.DOCUMENT_INS)
                                    break
                    self.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
                    self.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName) 
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - SUCCESSFULLY " + fullFileName)
                    FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = Constant.STATUS_OK, filename = self.fileName, externalSession = session)
                else:
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - EXISTS " + fullFileName)
                    FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = Constant.STATUS_OK, filename =self.fileName, externalSession = session)  
        except (FileNotFoundException, XSDNotFoundException, XMLNotFoundException) as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + str(e))
            FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = e.importStatus, filename =self.fileName, errorMessage=str(e), errorKey = Constant.ERROR_KEY_FILE)
        except MemoryError as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).exception(e)
            FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = Constant.STATUS_ERROR, filename =self.fileName, errorMessage='MemoryError', errorKey = Constant.ERROR_KEY_FILE)
        except Exception as e:
            #logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + url)
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).exception(e)
            FileDataDao().addOrModifyFileData(status = Constant.STATUS_PENDING, importStatus = Constant.STATUS_ERROR, filename =self.fileName, errorMessage=str(e), errorKey = Constant.ERROR_KEY_FILE)
        finally:
            session.remove()
            session.close()
    
    def validateIfSomeFilesNotExits(self, folder):
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_INS + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_PRE + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SCH + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SUMMARY + ".gz"):
            return True
    
    def saveFile(self, fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False, hardKey = None):
        if(hardKey is None):
            hardKey = key
        xmlFile= getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + hardKey + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
    
    def saveFile2(self, fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                self.saveFile(fileText, tagKey, key, mainTag, fullFileName)
                break
            except Exception as e:
                if(mainTagList[-1] == mainTag):
                    raise e
                else:
                    print(e)