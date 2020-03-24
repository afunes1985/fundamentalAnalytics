'''
Created on Jul 12, 2019

@author: afunes
'''
from dao.customFactDao import CustomFactDao
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from modelClass.customConcept import CustomConcept
from valueobject.constant import Constant


class ImporterCalculate(AbstractImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_CALCULATE, filename, replace, 'copyStatus', 'calculateStatus')
            
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