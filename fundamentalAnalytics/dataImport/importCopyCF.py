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
from importer.importerCopy import ImporterCopy
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.fileData import FileData
from tools.tools import createLog
from valueobject.constant import Constant
from dao.fileDataDao import FileDataDao


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

if __name__ == "__main__":
    replace = False
    threadNumber = 5
    conceptName = None
    maxProcessInQueue = 5
    Initializer()
    session = DBConnector().getNewSession()
    logging.info("START")
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/95574/0001437749-12-010410.txt"), session)
    fileDataList = FileDataDao().getFileData2('SGC', 'copyStatus', 'PENDING', session)
    threads = []    
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT COPY CF " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterCopy(fileData.fileName, replace)
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
            