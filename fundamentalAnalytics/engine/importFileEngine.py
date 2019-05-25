'''
Created on May 25, 2019

@author: afunes
'''
from _io import BytesIO, StringIO
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
    FileNotFoundException
from valueobject.constant import Constant
from valueobject.valueobject import ImportFileVO


class ImportFileEngine():
    
    @staticmethod
    def importMasterIndexFor(self, period, replaceMasterFile, session):
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
            #row = df.loc[1711736]
            threads = []
            s = Semaphore(1)
            for row in df.iterrows():
                filename = row[1]["Filename"]
                formType = row[1]["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):
                    importVO = ImportFileVO(filename, s)
                    t = threading.Thread(target=importVO.importFile)
                    t.start()
                    threads.append(t)
            for thread in threads:
                thread.join()
            print("FINISHED")
    
    @staticmethod
    def importFiles(filename, semaphore = None, reimport = False):
        try:
            if(semaphore is not None):
                semaphore.acquire()
            session = DBConnector().getNewSession()
            fileData = FileDataDao.getFileData(filename, session)
            if(fileData is None or (fileData.importStatus != "OK" and fileData.importStatus != "IMP FNF") or reimport == True ):
                FileDataDao.addOrModifyFileData("PENDING", "INIT", filename, session)
                fullFileName = Constant.CACHE_FOLDER + filename
                fullFileName = fullFileName[0: fullFileName.find(".txt")]
                logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("START - Processing index file " + fullFileName)   
                if(ImportFileEngine.validateIfSomeFilesNotExits(fullFileName)):
                    url = "https://www.sec.gov/Archives/" + filename
                    logging.getLogger(Constant.LOGGER_GENERAL).debug("URL " + url)
                    response = requests.get(url, timeout = 30) 
                    fileText = response.text
                    if not os.path.exists(fullFileName): 
                        os.makedirs(fullFileName)
                    ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
                    ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName)
                    #ImportFileEngine.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
                    ImportFileEngine.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - SUCCESSFULLY " + fullFileName)
                    FileDataDao.addOrModifyFileData("PENDING", "OK", filename, session)
                else:
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - EXISTS " + fullFileName)
                    FileDataDao.addOrModifyFileData("PENDING", "OK", filename, session)  
        except FileNotFoundException as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR FileNotFoundException " + url + " " + e.fileName)
            FileDataDao.addOrModifyFileData("PENDING", "IMP FNF", filename)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + url)
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).exception(e)
            FileDataDao.addOrModifyFileData("PENDING", "IMP ERROR", filename)
        finally:
            session.close()
            if(semaphore is not None):
                semaphore.release()
    
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
    def saveFile(fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False):
        xmlFile= getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + key + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
    
    @staticmethod        
    def saveFile2(fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                ImportFileEngine.saveFile(fileText, tagKey, key, mainTag, fullFileName)
            except Exception as e:
                print(e)