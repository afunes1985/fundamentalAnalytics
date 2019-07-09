'''
Created on 19 sep. 2018

@author: afunes
'''

from dao.factDao import FactDao
from importer.importer import AbstractImporter
from valueobject.constant import Constant
from importer.abstratFactImporter import AbstractFactImporter


class ImporterFact(AbstractImporter, AbstractFactImporter):

    def __init__(self, filename, replace, mainCache, conceptName = None):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_FACT, filename, replace)
        self.processCache = None
        self.mainCache = mainCache
        self.conceptName = conceptName
            
    def doImport2(self):
        self.processCache = self.initProcessCache(self.filename, self.session)
        self.fileData = self.completeFileData(self.fileData, self.processCache, self.filename, self.session)
        reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
        factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
        factVOList = self.setFactValues(factVOList, self.processCache)
        FactDao().addFact(factVOList, self.fileData, reportDict, self.session, self.replace)
        if(len(factVOList) != 0):
            self.fileData.status = Constant.STATUS_OK
        else: 
            self.fileData.status = Constant.STATUS_NO_DATA
    
    def addOrModifyFDError1(self, e):
        self.fileDataDao.addOrModifyFileData(status = e.status, filename = self.filename, errorMessage=str(e), errorKey = self.errorKey)
    
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(status = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:190], errorKey = self.errorKey)         
       
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(status = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey)   
            
    def skipOrProcess(self):
        if((self.fileData.importStatus == Constant.STATUS_OK and self.fileData.status != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False        
            
    