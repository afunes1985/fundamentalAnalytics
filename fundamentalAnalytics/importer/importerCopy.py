'''
Created on Jul 11, 2019

@author: afunes
'''
from dao.customFactDao import CustomFactDao
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from modelClass.customConcept import CustomConcept
from valueobject.constant import Constant


class ImporterCopy(AbstractImporter):
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_COPY, filename, replace, 'factStatus', 'copyStatus')
        self.customConceptList = GenericDao().getAllResult(objectClazz=CustomConcept, condition=(CustomConcept.fillStrategy == "COPY_CALCULATE"), session=self.session)
    
    def doImport2(self):
        return CustomFactEngine().copyToCustomFact2(fileData=self.fileData, customConceptList=self.customConceptList, session=self.session)
            
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(copyStatus=Constant.STATUS_ERROR, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey, externalSession=self.session)         
       
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(copyStatus=Constant.STATUS_INIT, filename=self.filename, errorKey=self.errorKey, externalSession=self.session)  
    
    def getPersistent(self, cfvVO):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=cfvVO.value, origin=cfvVO.origin, fileDataOID=cfvVO.fileDataOID,
                                    customConcept=cfvVO.customConcept, endDate=cfvVO.endDate, periodOID=cfvVO.periodOID, session=self.session)
        return customFactValue
    
    def deleteImportedObject(self):
        CustomFactDao().deleteCFVByFD(self.fileData.OID, "COPY", self.session)
