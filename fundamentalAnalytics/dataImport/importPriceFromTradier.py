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


#logging.basicConfig(level=logging.ERROR)
Initializer()
session = DBConnector().getNewSession()
#fileDataList = FileDataDao().getFileData5( statusAttr='priceStatus', statusValue='NO_DATA', session=session, errorMessage2='')
fileDataList = FileDataDao().getFileData6( statusAttr='fileName', statusValue='edgar/data/1000229/0001000229-18-000071.txt', session=session)
importerExecutor = ImporterExecutor(threadNumber=5, maxProcessInQueue=5, replace=True, isSequential=False, importerClass=ImporterPrice)
importerExecutor.execute(fileDataList)
