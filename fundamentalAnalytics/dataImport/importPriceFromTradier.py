import logging

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerPrice import ImporterPrice


#logging.basicConfig(level=logging.ERROR)
Initializer()
session = DBConnector().getNewSession()
#fileDataList = FileDataDao().getFileData4( statusAttr='priceStatus', statusValue='ERROR', statusAttr2='entityStatus', statusValue2='OK', session=session)
fileDataList = FileDataDao().getFileData5( statusAttr='priceStatus', statusValue='ERROR', session=session, errorMessage2='%= 60%')
importerExecutor = ImporterExecutor(threadNumber=3, maxProcessInQueue=5, replace=True, isSequential=False, importerClass=ImporterPrice)
importerExecutor.execute(fileDataList)
