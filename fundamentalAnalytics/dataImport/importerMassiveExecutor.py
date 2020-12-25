'''
Created on Nov 20, 2020

@author: afunes
'''
from abc import abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore
import time

from sqlalchemy import event
from sqlalchemy.engine.base import Engine

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
from importer.importerCopy import ImporterCopy
from tools.tools import createLog
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterMassiveExecutor(object):
    
    logger = None
    parameters = ""

    def __init__(self, threadNumber=1, maxProcessInQueue=5, replace=False, isSequential=True):
        self.initLogger()
        logging.info("START")
        self.executor = ThreadPoolExecutor(max_workers=threadNumber)
        self.semaphore = BoundedSemaphore(maxProcessInQueue)
        self.replace = replace
        self.isSequential = isSequential
        
        
    def execute(self, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        statusDict = ConstantStatus().getStatusDict()
        for status in statusDict:
            prevStatus = statusDict[status].get('prevStatus')
            importerClass = statusDict[status].get('importerClass')
            print('status ' + status)
            if(status == ConstantStatus.ENTITY_FACT_STATUS):
                fileDataList = FileDataDao().getFileData3(statusAttr=status, statusValue=Constant.STATUS_PENDING, statusAttr2=prevStatus, statusValue2=Constant.STATUS_OK, session=session, listed=True)
            elif(prevStatus is not None):
                fileDataList = FileDataDao().getFileData4(statusAttr=status, statusValue=Constant.STATUS_PENDING, statusAttr2=prevStatus, statusValue2=Constant.STATUS_OK, session=session)
            else:
                fileDataList = FileDataDao().getFileData6(statusAttr=status, statusValue=Constant.STATUS_PENDING, session=session)
            self.logger.info("READY TO IMPORT " + status + " FILEDATA - count " + str(len(fileDataList)))
            if (self.isSequential):
                for fileData in fileDataList:
                    try:
                        fi = importerClass(filename=fileData.fileName, replace=self.replace)
                        fi.doImport()
                    except Exception as e:
                        self.logger.exception(e)
#         else:
#             for fileData in fileDataList:
#                 try:
#                     try:
#                         self.semaphore.acquire()
#                         fi = self.importerClass(fileData.fileName, self.replace)
#                         future = self.executor.submit(fi.doImport)
#                     except Exception as e:
#                         self.semaphore.release()
#                         self.logger.exception(e)
#                     else:
#                         future.add_done_callback(lambda x: self.semaphore.release())
#                 except Exception as e:
#                         self.logger.exception("ERROR " + fileData.fileName + " " + str(e))
#         self.logger.info("PROCESS FINISHED FOR " + str(len(fileDataList)) + " FILEDATA")
                        
    @abstractmethod
    def initLogger(self):
        if (ImporterMassiveExecutor.logger is None):
            ImporterMassiveExecutor.logger = createLog(self.__class__.__name__, logging.INFO)
            
# logging.basicConfig()
# logger = logging.getLogger("myapp.sqltime")
# logger.setLevel(logging.DEBUG)
#       
# @event.listens_for(Engine, "before_cursor_execute")
# def before_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     conn.info.setdefault('query_start_time', []).append(time.time())
#     logger.debug("Start Query: %s, %s", statement, parameters)
#       
# @event.listens_for(Engine, "after_cursor_execute")
# def after_cursor_execute(conn, cursor, statement,
#                         parameters, context, executemany):
#     total = time.time() - conn.info['query_start_time'].pop(-1)
#     #logger.debug("Query Complete!")
#     logger.debug("Total Time: %f", total)
            