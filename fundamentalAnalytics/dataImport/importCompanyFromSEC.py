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
    #fileDataList = FileDataDao().getLastFileData(session=session)
    #fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='edgar/data/1002910/0001002910-18-000119.txt', session = session)
    fileDataList = FileDataDao().getFileData4(statusAttr2='fileStatus', statusValue2='OK', statusAttr='companyStatus', statusValue='PENDING', session=session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterCompany)
    importerExecutor.execute(fileDataList)
            
            
