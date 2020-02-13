import logging
import time

from sqlalchemy import event
from sqlalchemy.engine.base import Engine

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.entityFactDao import EntityFactDao
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerPrice import ImporterPrice


#logging.basicConfig(level=logging.DEBUG)
Initializer()
session = DBConnector().getNewSession()
fileDataList = FileDataDao().getFileData5( statusAttr='priceStatus', statusValue='ERROR', session=session, errorMessage2='Price not found for')
importerExecutor = ImporterExecutor(threadNumber=5, maxProcessInQueue=5, replace=True, isSequential=False, importerClass=ImporterPrice)
importerExecutor.execute(fileDataList)
