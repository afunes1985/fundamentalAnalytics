'''
Created on 22 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from importer.importerExpression import ImporterExpression

if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    # fileDataList = GenericDao().getAllResult(FileData, and_(FileData.status.__eq__("OK"), FileData.expressionStatus.__eq__("PENDING")), session)
    # fileDataList = GenericDao().getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("ERROR")), session)
    # fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/320193/0000320193-18-000070.txt"), session)
    fileDataList = FileDataDao().getFileData2('SGC', 'expressionStatus', 'OK', session)
    importerExecutor = ImporterExecutor(threadNumber=1, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterExpression)
    importerExecutor.execute(fileDataList)
    
            
