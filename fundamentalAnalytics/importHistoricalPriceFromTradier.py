from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore
import time

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao, Dao
from engine.importPriceEngine import ImportPriceEngine
from modelClass.concept import Concept
from modelClass.entityFact import EntityFact
from modelClass.price import Price
from dao.entityFactDao import EntityFactDao


logging.basicConfig(level=logging.DEBUG)
threadNumber = 3
maxProcessInQueue = 15
executor = ThreadPoolExecutor(max_workers=threadNumber)
semaphore = BoundedSemaphore(maxProcessInQueue)
Initializer()
session = DBConnector().getNewSession()
conceptName = 'EntityCommonStockSharesOutstanding'
entityFactList = EntityFactDao().getEntityFactList(ticker="", conceptName = conceptName, priceStatus = "ERROR", session = session)
for efv in entityFactList:
    ipe = ImportPriceEngine(efv[0], efv[1], efv[2], efv[3], efv[4])
    #fi.doImport(replace)
    semaphore.acquire()
    try:
        future = executor.submit(ipe.doImportPrice)
    except:
        semaphore.release()
    else:
        future.add_done_callback(lambda x: semaphore.release())