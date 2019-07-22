'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerCopy import ImporterCopy

if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    fileDataList = FileDataDao().getFileData2('SGC', 'copyStatus', 'OK', session)
    importerExecutor = ImporterExecutor(threadNumber = 1, maxProcessInQueue = 5, replace = True, isSequential = True, importerClass= ImporterCopy)
    importerExecutor.execute(fileDataList)
    