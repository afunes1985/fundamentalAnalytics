'''
Created on 22 ago. 2018

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from nt import listdir
from threading import BoundedSemaphore
import traceback

import pandas
from sqlalchemy.sql.expression import and_
import xmltodict

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from modelClass.fileData import FileData
from tools.tools import createLog, getXSDFileFromCache
from valueobject.constant import Constant
from importer.importerEntityFact import ImporterEntityFact


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
        except Exception:
            traceback.print_exc()
    return mainCache

if __name__ == "__main__":
    replace = False
    threadNumber = 3
    conceptName = 'EntityCommonStockSharesOutstanding'
    maxProcessInQueue = 10
    Initializer()
    session = DBConnector().getNewSession()
    createLog(Constant.LOGGER_GENERAL, logging.INFO)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    fileDataList = GenericDao().getAllResult(FileData, and_(FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("INIT")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/1016708/0001477932-18-002398.txt"), session)
    threads = []    
    mainCache = initMainCache()
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT ENTITY FACT " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterEntityFact(fileData.fileName, replace, mainCache, conceptName)
                #fi.doImport()
                future = executor.submit(fi.doImport)
            except:
                semaphore.release()
            else:
                #pass
                future.add_done_callback(lambda x: semaphore.release())
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            