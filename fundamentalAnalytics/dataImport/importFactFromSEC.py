'''
Created on 22 ago. 2018

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from nt import listdir
from threading import Semaphore, BoundedSemaphore
import threading
from time import sleep
import traceback

import pandas
from sqlalchemy.sql.expression import and_, or_
import xmltodict

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.factImporterEngine import FactImporterEngine
from modelClass.company import Company
from modelClass.fileData import FileData
from tools.tools import createLog, getXSDFileFromCache
from valueobject.constant import Constant


def initMainCache():
    mainCache = {}
    for xsdFileName in listdir(Constant.CACHE_FOLDER + "xsd"):
        try:
            xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, None)
            xsdDict = xmltodict.parse(xsdFile)
            xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
            xsdDF.set_index("@id", inplace=True)
            xsdDF.head()
            mainCache[xsdFileName] = xsdDF
            print(xsdFileName)
        except Exception as e:
            traceback.print_exc()
    return mainCache

if __name__ == "__main__":
    COMPANY_TICKER = None
    replace = False
    threadNumber = 3
    conceptName = 'EntityCommonStockSharesOutstanding'
    maxProcessInQueue = 20
    Initializer()
    session = DBConnector().getNewSession()
    if (COMPANY_TICKER is not None):
        company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
    else:
        company = None
    
    createLog(Constant.LOGGER_GENERAL, logging.INFO)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    
    fileDataList = GenericDao.getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("OK")), session)
    #fileDataList = GenericDao.getAllResult(FileData, and_(FileData.fileName == "edgar/data/1016708/0001477932-18-002398.txt"), session)
    threads = []    
    mainCache = initMainCache()
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    for fileData in fileDataList:
        try:
            if (conceptName is None):
                fi = FactImporterEngine(fileData.fileName, replace, mainCache)
                #fi.doImport(replace)
                executor.submit(fi.doImport)
            else:
                
                fi = FactImporterEngine(fileData.fileName, replace, mainCache, conceptName)
                #fi.doImport(replace)
                semaphore.acquire()
                try:
                    future = executor.submit(fi.doImportEntityFactByConcept)
                except:
                    semaphore.release()
                else:
                    future.add_done_callback(lambda x: semaphore.release())
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            