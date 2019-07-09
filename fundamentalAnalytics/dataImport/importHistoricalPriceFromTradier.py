from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao, Dao
from dao.entityFactDao import EntityFactDao
from importer.importerPrice import ImporterPrice
from valueobject.constant import Constant


logging.basicConfig(level=logging.DEBUG)
threadNumber = 3
maxProcessInQueue = 15
executor = ThreadPoolExecutor(max_workers=threadNumber)
semaphore = BoundedSemaphore(maxProcessInQueue)
Initializer()
session = DBConnector().getNewSession()
conceptName = 'EntityCommonStockSharesOutstanding'
entityFactList = EntityFactDao().getEntityFactList(ticker="", conceptName = conceptName, priceStatus = Constant.STATUS_PENDING, session = session)
for efv in entityFactList:
    ipe = ImporterPrice(efv[0], efv[1], efv[2], efv[3], efv[4])
    semaphore.acquire()
    try:
        future = executor.submit(ipe.doImport)
    except:
        semaphore.release()
    else:
        future.add_done_callback(lambda x: semaphore.release())