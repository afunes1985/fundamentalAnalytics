'''
Created on 22 ago. 2018

@author: afunes
'''

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerEntityFact import ImporterEntityFact

if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    #fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='edgar/data/1156039/0001156039-17-000011.txt', session=session)
    fileDataList = FileDataDao().getFileData5( statusAttr='entityStatus', statusValue='ERROR', session=session, errorMessage2='%uncon%')
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterEntityFact)
    importerExecutor.execute(fileDataList)
            
