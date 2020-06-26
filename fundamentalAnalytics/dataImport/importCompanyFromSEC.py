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
from valueobject.constant import Constant


if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    #fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='edgar/data/1089598/0001014897-19-000042.txt', session = session)
    fileDataList = FileDataDao().getLastFileData(session)
#     fileDataList = FileDataDao().getFileDataByError(errorKey=Constant.ERROR_KEY_COMPANY, errorMessage='%not well-formed (invalid token)%', session=session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterCompany)
    importerExecutor.execute(fileDataList)
            
            
