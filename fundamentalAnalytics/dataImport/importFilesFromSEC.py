'''
Created on 9 nov. 2017

@author: afunes
'''
from _io import BytesIO, StringIO
import gzip
import logging
import os
from threading import Lock, Semaphore
import threading

import pandas
import requests
from sqlalchemy.sql.expression import and_, or_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from modelClass.period import QuarterPeriod
from tools.tools import getBinaryFileFromCache, createLog, getXMLFromText, \
    FileNotFoundException, addOrModifyFileData
from valueobject.constant import Constant

#Import fileData in PENDING state (OK, ERROR, FileNotFount (FNF))
class ImportFIlesFromSEC():
    def importMasterIndexFor(self, period, replace, session):
        file = getBinaryFileFromCache(Constant.CACHE_FOLDER + 'master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                    "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/master.gz")
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
                    s.acquire()
                    importVO = ImportVO(filename, s)
                    t = threading.Thread(target=importVO.importFile)
                    t.start()
                    threads.append(t)
            for thread in threads:
                thread.join()
            print("FINISHED")
                    
    
    

class ImportVO():
    filenameToImport = None
    def __init__(self, filename, s):
        self.filenameToImport = filename
        self.semaphore = s
        
    def importFile(self):
        #self.semaphore.acquire()
        self.importFiles(self.filenameToImport)
        self.semaphore.release()
        
    def importFiles(self, filename):
        try:
            session = DBConnector().getNewSession()
            fileData = Dao.getFileData(filename, session)
            if(fileData is None or fileData.importStatus != "OK" and fileData.importStatus != "IMP FNF" ):
                addOrModifyFileData("PENDING", "INIT", filename, session)
                fullFileName = Constant.CACHE_FOLDER + filename
                fullFileName = fullFileName[0: fullFileName.find(".txt")]
                logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("START - Processing index file " + fullFileName)   
                if(self.validateIfSomeFilesNotExits(fullFileName)):
                    url = "https://www.sec.gov/Archives/" + filename
                    logging.getLogger(Constant.LOGGER_GENERAL).debug("URL " + url)
                    response = requests.get(url, timeout = 30) 
                    fileText = response.text
                    if not os.path.exists(fullFileName):
                        os.makedirs(fullFileName)
                    self.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
                    self.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName)
                    self.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
                    self.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - SUCCESSFULLY " + fullFileName)
                    addOrModifyFileData("PENDING", "OK", filename, session)
                else:
                    logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("END - EXISTS " + fullFileName)
                    addOrModifyFileData("PENDING", "OK", filename, session)  
        except FileNotFoundException as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR FileNotFoundException " + url + " " + e.fileName)
            addOrModifyFileData("PENDING", "IMP FNF", filename)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).debug("ERROR " + url)
            logging.getLogger(Constant.LOGGER_IMPORT_GENERAL).exception(e)
            addOrModifyFileData("PENDING", "IMP ERROR", filename)
        finally:
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
        
            
             
    def saveFile(self, fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False):
        xmlFile= getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + key + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
            
    def saveFile2(self, fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                self.saveFile(fileText, tagKey, key, mainTag, fullFileName)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 3)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    #periodList = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2020, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter > 3)), QuarterPeriod.year > 2017)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    for period in periodList:
        ImportFIlesFromSEC().importMasterIndexFor(period, False, session)