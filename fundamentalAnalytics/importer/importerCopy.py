'''
Created on Jul 11, 2019

@author: afunes
'''
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from valueobject.constant import Constant


class ImporterCopy(AbstractImporter):
    
    def __init__(self, filename, replace, customConceptList):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_COPY, filename, replace)
        self.customConceptList = []
        for cc in customConceptList:
            self.customConceptList.append(self.session.merge(cc))
    
    def doImport2(self):
        cfvCount= CustomFactEngine().copyToCustomFact2(fileData = self.fileData, customConceptList = self.customConceptList, session = self.session)
        if(cfvCount != 0):
            self.fileData.copyStatus = Constant.STATUS_OK
        else: 
            self.fileData.copyStatus = Constant.STATUS_NO_DATA
            
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(copyStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey)         
       
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(copyStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey)   
            
    def skipOrProcess(self):
        if((self.fileData.status == Constant.STATUS_OK and self.fileData.copyStatus != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False   