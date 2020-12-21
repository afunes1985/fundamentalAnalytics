'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerCompany import ImporterCompany


if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue='ImporterCompany:edgar/data/788784/0000788784-20-000010.txt', session = session)
#     fileDataList = FileDataDao().getLastFileData(session)
#     fileDataList = FileDataDao().getFileDataByError(errorKey=Constant.ERROR_KEY_COMPANY, errorMessage='%not well-formed (invalid token)%', session=session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterCompany)
    importerExecutor.execute(fileDataList)
            
            
