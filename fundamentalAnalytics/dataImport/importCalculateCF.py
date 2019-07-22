'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerCalculate import ImporterCalculate


if __name__ == "__main__":
    threadNumber = 1
    maxProcessInQueue = 5
    Initializer()
    session = DBConnector().getNewSession()
    fileDataList = FileDataDao().getFileData2('SGC', 'calculateStatus', 'PENDING', session)
    importerExecutor = ImporterExecutor(threadNumber = 1, maxProcessInQueue = 5, replace = False, isSequential = True, importerClass= ImporterCalculate)
    importerExecutor.execute(fileDataList)
