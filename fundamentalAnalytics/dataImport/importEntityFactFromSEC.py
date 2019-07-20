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
from importer.importerEntityFact import ImporterEntityFact
from valueobject.constant import Constant


if __name__ == "__main__":
    replace = False
    threadNumber = 3
    conceptName = 'EntityCommonStockSharesOutstanding'
    maxProcessInQueue = 10
    Initializer()
    session = DBConnector().getNewSession()
    logging.info("START")
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("INIT")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/1016708/0001477932-18-002398.txt"), session)
    fileDataList = FileDataDao().getFileData2('SGC', 'entityStatus', 'PENDING', session)
    threads = []    
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT ENTITY FACT " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterEntityFact(fileData.fileName, replace, conceptName)
                fi.doImport()
                #future = executor.submit(fi.doImport)
            except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
                semaphore.release()
            else:
                pass
                #future.add_done_callback(lambda x: semaphore.release())
                semaphore.release()
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            