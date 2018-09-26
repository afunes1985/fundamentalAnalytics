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
from modelClass.period import QuarterPeriod
from tools.tools import getBinaryFileFromCache, createLog
from valueobject.constant import Constant


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
            s = Semaphore(10)
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
            fullFileName = Constant.CACHE_FOLDER + filename
            fullFileName = fullFileName[0: fullFileName.find(".txt")]
            print(fullFileName)
                url = "https://www.sec.gov/Archives/" + filename
                print(url)
                response = requests.get(url, timeout = 30) 
                fileText = response.text
                if not os.path.exists(fullFileName):
                    os.makedirs(fullFileName)
                self.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
                self.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName)
                self.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
                self.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
            else:
                print("EXISTS " + fullFileName)
        except Exception as e:
                print(e)
           
    def validateIfSomeFilesNotExits(self, folder):
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_INS + ".gz"):
            
             
    def saveFile(self, fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False):
        xmlFile= self.getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + key + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
            
    def saveFile2(self, fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                self.saveFile(fileText, tagKey, key, mainTag, fullFileName)
            except Exception as e:
                print(e)


    def getXMLFromText(self, fileText, tagKey, key, mainTag, skipIfNotExists):
        point1 = fileText.find("<" + tagKey + ">" + key, 0, len(fileText))
        if(point1 == -1 and skipIfNotExists == False):
            raise Exception("Key " + key + " doesn't found")
        elif(point1 == -1):
            return None
        point2 = fileText.find("<" + mainTag +">", point1, len(fileText)) + len("<" + mainTag + ">")+1
        
        if (0 < point2 - point1 < 150 ):
            point3 = fileText.find("</" + mainTag + ">", point2, len(fileText))
            xmlText = fileText[point2:point3]
            return xmlText
        else:
            return None
        
if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 1)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    #periodList = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2018, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter <= 3)), QuarterPeriod.year > 2015)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    logging.info("START")
    createLog('general', logging.DEBUG)
    for period in periodList:
        ImportFIlesFromSEC().importMasterIndexFor(period, False, session)