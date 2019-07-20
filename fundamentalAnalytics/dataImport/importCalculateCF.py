'''
Created on 22 ago. 2018

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore


from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from importer.importerCalculate import ImporterCalculate
from tools.tools import createLog
from valueobject.constant import Constant


# 
# logging.basicConfig()
# logger = logging.getLogger("myapp.sqltime")
# logger.setLevel(logging.DEBUG)
#    
# @event.listens_for(Engine, "before_cursor_execute")
# def before_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     conn.info.setdefault('query_start_time', []).append(time.time())
#     #logger.debug("Start Query: %s", statement)
#    
# @event.listens_for(Engine, "after_cursor_execute")
# def after_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     total = time.time() - conn.info['query_start_time'].pop(-1)
#     #if (total > 1):
#     logger.debug("Query: %s", statement)
#     logger.debug("Parameters: %s", parameters)
#     logger.debug("Total Time: %f", total)
#     
if __name__ == "__main__":
    replace = False
    threadNumber = 1
    conceptName = None
    maxProcessInQueue = 5
    Initializer()
    session = DBConnector().getNewSession()
    createLog(Constant.LOGGER_GENERAL, logging.INFO)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.copyStatus.__eq__("OK"), FileData.calculateStatus.__eq__("PENDING")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("ERROR")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/894315/0001564590-17-001905.txt"), session)
    fileDataList = FileDataDao().getFileData2('SGC', 'calculateStatus', 'PENDING', session)
    threads = []    
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT CALCULATE CF " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterCalculate(fileData.fileName, replace)
                fi.doImport()
                #future = executor.submit(fi.doImport)
            except Exception as e:
                semaphore.release()
                logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            else:
                #future.add_done_callback(lambda x: semaphore.release())
                pass
                semaphore.release()
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            