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


logging.basicConfig(level=logging.DEBUG)
threadNumber = 1
maxProcessInQueue = 20
executor = ThreadPoolExecutor(max_workers=threadNumber)
semaphore = BoundedSemaphore(maxProcessInQueue)
Initializer()
session = DBConnector().getNewSession()
conceptName = 'EntityCommonStockSharesOutstanding'
concept = GenericDao.getOneResult(Concept, (Concept.conceptName == conceptName), session, True)
entityFactList = GenericDao.getAllResult(EntityFact, EntityFact.concept.__eq__(concept), session)
for entityFact in entityFactList:
    for efv in entityFact.entityFactValueList:
        ipe = ImportPriceEngine(efv)
        #fi.doImport(replace)
        semaphore.acquire()
        try:
            future = executor.submit(ipe.doImportPrice)
        except:
            semaphore.release()
        else:
            future.add_done_callback(lambda x: semaphore.release())