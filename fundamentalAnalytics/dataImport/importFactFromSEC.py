'''
Created on 22 ago. 2018

@author: afunes
'''
import logging

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerFact import ImporterFact


if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    Initializer()
    session = DBConnector().getNewSession()
    #statusAttr='documentFiscalYearFocus', statusValue='2018'
    fileDataList = FileDataDao().getFileData6( statusAttr='fileName', statusValue='edgar/data/1385280/0001193125-13-202887.txt', session=session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterFact)
    importerExecutor.execute(fileDataList)
            
            
