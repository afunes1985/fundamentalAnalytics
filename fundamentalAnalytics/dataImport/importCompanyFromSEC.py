'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerCompany import ImporterCompany
from modelClass.fileData import FileData


if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='edgar/data/1460602/0001062993-20-001299.txt', session = session)
    #fileDataList = FileDataDao().getLastFileData(session)
    #fileDataList = FileDataDao().getFileData6( statusAttr='companyStatus', statusValue='ERROR', session=session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterCompany)
    importerExecutor.execute(fileDataList)
            
            
