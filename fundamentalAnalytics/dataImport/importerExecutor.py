'''
Created on Jul 21, 2019

@author: afunes
'''
from abc import abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore
import time

from sqlalchemy import event
from sqlalchemy.engine.base import Engine

from importer.importerCopy import ImporterCopy
from tools.tools import createLog


class ImporterExecutor(object):
    
    logger = None
    parameters = ""

    def __init__(self, threadNumber, maxProcessInQueue, replace, isSequential, importerClass):
        self.initLogger()
        logging.info("START")
        self.executor = ThreadPoolExecutor(max_workers=threadNumber)
        self.semaphore = BoundedSemaphore(maxProcessInQueue)
        self.replace = replace
        self.isSequential = isSequential
        self.importerClass = importerClass
        
        
    def execute(self, fileDataList):
        self.logger.info("READY TO IMPORT " + str(len(fileDataList)) + " FILEDATA")
        if (self.isSequential):
            for fileData in fileDataList:
                try:
                    fi = self.importerClass(fileData.fileName, self.replace)
                    fi.doImport()
                except Exception as e:
                    self.logger.exception(e)
        else:
            for fileData in fileDataList:
                try:
                    try:
                        self.semaphore.acquire()
                        fi = ImporterCopy(fileData.fileName, self.replace)
                        future = self.executor.submit(fi.doImport)
                    except Exception as e:
                        self.semaphore.release()
                        self.logger.exception(e)
                    else:
                        future.add_done_callback(lambda x: self.semaphore.release())
                except Exception as e:
                        self.logger.exception("ERROR " + fileData.fileName + " " + str(e))
                        
    @abstractmethod
    def initLogger(self):
        if (ImporterExecutor.logger is None):
            ImporterExecutor.logger = createLog(self.__class__.__name__, logging.INFO)
            
logging.basicConfig()
logger = logging.getLogger("myapp.sqltime")
logger.setLevel(logging.DEBUG)
   
@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start Query: %s, %s", statement, parameters)
   
@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    #logger.debug("Query Complete!")
    logger.debug("Total Time: %f", total)
            