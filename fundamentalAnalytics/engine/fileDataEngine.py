'''
Created on Jun 11, 2020

@author: afunes
'''
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from tools import tools
from valueobject.constantStatus import ConstantStatus


class FileDataEngine():
    
    def getFileStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.FILE_STATUS)
        return tools.convertListToDDDict(resultList)

    def getCompanyStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.COMPANY_STATUS)
        return tools.convertListToDDDict(resultList)
    
    def getPriceStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.PRICE_STATUS)
        return tools.convertListToDDDict(resultList)
        
    def getEntityFactStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.ENTITY_FACT_STATUS)
        return tools.convertListToDDDict(resultList)
    
    def getFactStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.FACT_STATUS)
        return tools.convertListToDDDict(resultList)
    
    def getCopyStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.COPY_STATUS)
        return tools.convertListToDDDict(resultList)

    def getCalculateStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.CALCULATE_STATUS)
        return tools.convertListToDDDict(resultList)    
    
    def getExpressionStatusDict(self):  
        resultList = FileDataDao().getStatusList(statusAttr=ConstantStatus.EXPRESSION_STATUS)
        return tools.convertListToDDDict(resultList)   
    
    def processFileData(self, action, fileName, importerClass):
        if action == 'Reprocess':
            importer = importerClass(filename=fileName, replace=True)
            importer.doImport()
        elif action == 'Process':
            importer = importerClass(filename=fileName, replace=False)
            importer.doImport()
        elif action == 'Delete':
            importer = importerClass(filename=fileName, replace=True)
            importer.deleteImportedObject()
            importer.addOrModifyFDPending()
            
    def processFileData2(self, action, fileDataList, importerClass):
        if action == 'Reprocess':
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=importerClass)
            importerExecutor.execute(fileDataList)
        elif action == 'Process':
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=importerClass)
            importerExecutor.execute(fileDataList)
        elif action == 'Delete':
            pass
#             importer = importerClass(filename=fileName, replace=True)
#             importer.deleteImportedObject()
#             importer.addOrModifyFDPending()