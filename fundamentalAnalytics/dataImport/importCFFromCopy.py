'''
Created on 22 ago. 2018

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore

from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from modelClass.customConcept import CustomConcept
from modelClass.fileData import FileData
from tools.tools import createLog
from valueobject.constant import Constant
from importer.importerCopy import ImporterCopy

from sqlalchemy import event
from sqlalchemy.engine import Engine
import time
import logging

# logging.basicConfig()
# logger = logging.getLogger("myapp.sqltime")
# logger.setLevel(logging.DEBUG)
# 
# @event.listens_for(Engine, "before_cursor_execute")
# def before_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     conn.info.setdefault('query_start_time', []).append(time.time())
#     logger.debug("Start Query: %s", statement)
# 
# @event.listens_for(Engine, "after_cursor_execute")
# def after_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     total = time.time() - conn.info['query_start_time'].pop(-1)
#     logger.debug("Query Complete!")
#     logger.debug("Total Time: %f", total)


def initMainCache(session):
    customConceptList = GenericDao().getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "COPY_CALCULATE"), session = session)
    return customConceptList

if __name__ == "__main__":
    replace = False
    threadNumber = 5
    conceptName = None
    maxProcessInQueue = 5
    Initializer()
    session = DBConnector().getNewSession()
    createLog(Constant.LOGGER_GENERAL, logging.INFO)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    fileDataList = GenericDao().getAllResult(FileData, and_(FileData.status.__eq__("OK"), FileData.copyStatus.__eq__("PENDING")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("ERROR")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/1013488/0001564590-18-011253.txt"), session)
    threads = []    
    customConceptList = initMainCache(session)
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT COPY CF " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterCopy(fileData.fileName, replace, customConceptList)
                #fi.doImport()
                future = executor.submit(fi.doImport)
            except Exception as e:
                semaphore.release()
                logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            else:
                future.add_done_callback(lambda x: semaphore.release())
                #pass
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            