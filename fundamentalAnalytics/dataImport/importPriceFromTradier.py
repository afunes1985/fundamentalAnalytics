import logging
import time

from sqlalchemy import event
from sqlalchemy.engine.base import Engine

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.entityFactDao import EntityFactDao
from importer.importerPrice import ImporterPrice
 

#logging.basicConfig(level=logging.DEBUG)
Initializer()
session = DBConnector().getNewSession()
conceptName = 'EntityCommonStockSharesOutstanding'
entityFactList = EntityFactDao().getEntityFactList(ticker="INTC", conceptName = conceptName, priceStatus = "PENDING", session = session)
for efv in entityFactList:
    ipe = ImporterPrice(efv[0], efv[1], efv[2], efv[3], efv[4], True)
    ipe.doImport()
