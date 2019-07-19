'''
Created on Jul 12, 2019

@author: afunes
'''
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from valueobject.constant import Constant


class ImporterCalculate(AbstractImporter):

    def __init__(self, filename, replace, cacheDict):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_CALCULATE, filename, replace, 'copyStatus', 'calculateStatus')
        self.customConceptList = []
        for cc in cacheDict["customConceptList"]:
            self.customConceptList.append(self.session.merge(cc))
            
    def doImport2(self):
        return CustomFactEngine().calculateMissingQTDValues2(fileData = self.fileData, customConceptList = self.customConceptList, session=self.session)

    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(calculateStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey)         
       
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(calculateStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey)   
            
    def getPersistent(self, cfvVO):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=cfvVO.value, origin=cfvVO.origin, fileDataOID=cfvVO.fileDataOID,
                                    customConcept=cfvVO.customConcept,  endDate=cfvVO.endDate, session=self.session)
        return customFactValue  
        