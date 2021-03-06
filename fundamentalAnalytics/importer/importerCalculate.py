'''
Created on Jul 12, 2019

@author: afunes
'''
from dao.customFactDao import CustomFactDao
from dao.dao import GenericDao, Dao
from dao.fileDataDao import FileDataDao
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from modelClass.customConcept import CustomConcept
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterCalculate(AbstractImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_CALCULATE, filename, replace, ConstantStatus.COPY_STATUS, ConstantStatus.CALCULATE_STATUS)
            
    def doImport2(self):
        customConceptList = GenericDao().getAllResult(objectClazz = CustomConcept, condition = (CustomConcept.fillStrategy == "COPY_CALCULATE"), session = self.session)
        customConceptListFilled= CustomFactDao().getCustomConceptFilled(self.fileData.OID, self.session)
        self.customConceptListMissing =[x for x in customConceptList if not x.conceptName in customConceptListFilled]
        self.customConceptListResult = CustomFactEngine().calculateMissingQTDValues2(fileData = self.fileData, customConceptList = self.customConceptListMissing, session=self.session)
        return self.customConceptListResult

    def getPersistent(self, cfvVO):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=cfvVO.value, origin=cfvVO.origin, fileDataOID=cfvVO.fileDataOID,
                                    customConcept=cfvVO.customConcept,  endDate=cfvVO.endDate, session=self.session)
        return customFactValue  
    
    def deleteImportedObject(self):
        CustomFactDao().deleteCFVByFD(self.fileData.OID, "CALCULATED", self.session)

    def getMissingObjects(self):
        listResult = [x.customConcept.conceptName for x in self.customConceptListResult]
        print([x.conceptName for x in self.customConceptListMissing if not x.conceptName in listResult])
        return [x.conceptName for x in self.customConceptListMissing if not x.conceptName in listResult]
    
    def setFileDataStatus(self, voList):
        if(voList is not None):
            missingObjects = self.getMissingObjects()
            if(len(missingObjects) > 0):
                setattr(self.fileData, self.actualStatus, Constant.STATUS_WARNING)
                FileDataDao().setErrorMessage(errorMessage=str(missingObjects)[0:149], errorKey=self.errorKey, fileData=self.fileData)
            else:    
                setattr(self.fileData, self.actualStatus, Constant.STATUS_OK)
        else: 
            setattr(self.fileData, self.actualStatus, Constant.STATUS_OK) 
        Dao().addObject(objectToAdd=self.fileData, session=self.session, doCommit=True)