import logging

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerPrice import ImporterPrice


#logging.basicConfig(level=logging.ERROR)
Initializer()
session = DBConnector().getNewSession()
fileDataList = FileDataDao().getFileData6( statusAttr='fileName', statusValue='edgar/data/1006269/0001558370-20-002499.txt',session=session)
#fileDataList = FileDataDao().getFileData5( statusAttr='priceStatus', statusValue='ERROR', session=session, errorMessage2='%= 60%')
importerExecutor = ImporterExecutor(threadNumber=3, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterPrice)
importerExecutor.execute(fileDataList)
