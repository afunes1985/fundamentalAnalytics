'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerFact import ImporterFact

if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    Initializer()
    session = DBConnector().getNewSession()
    #statusAttr='documentFiscalYearFocus', statusValue='2018'
    fileDataList = FileDataDao().getFileData2( statusAttr='documentFiscalYearFocus', statusValue='2016', session=session, limit=None)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterFact)
    importerExecutor.execute(fileDataList)
            
            
