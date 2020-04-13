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
    fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='edgar/data/4515/0000006201-20-000023.txt', session = session)
    #fileDataList = FileDataDao().getFileData4(statusAttr2='fileStatus', statusValue2='OK', statusAttr='companyStatus', statusValue='ERROR', session=session)
    #fileDataList = FileDataDao().getFileData5( statusAttr='priceStatus', statusValue='edgar/data/100517/0000100517-20-000010.txt', session=session, errorMessage2='%Price not%')
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterCompany)
    importerExecutor.execute(fileDataList)
            
            
