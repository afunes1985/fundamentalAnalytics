'''
Created on Jul 11, 2019

@author: afunes
'''
from sqlalchemy.sql.elements import or_

from dao.customFactDao import CustomFactDao
from dao.dao import GenericDao
from engine.customFactEngine import CustomFactEngine
from importer.abstractImporter import AbstractImporter
from modelClass.customConcept import CustomConcept
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterCopy(AbstractImporter):
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, errorKey=Constant.ERROR_KEY_COPY, filename=filename, replace=replace, previousStatus=ConstantStatus.FACT_STATUS, 
                                  actualStatus=ConstantStatus.COPY_STATUS)
        self.customConceptList = GenericDao().getAllResult(objectClazz=CustomConcept, condition=(or_(CustomConcept.fillStrategy == "COPY_CALCULATE", CustomConcept.fillStrategy == "COPY")), session=self.session)
    
    def doImport2(self):
        return CustomFactEngine().copyToCustomFact2(fileData=self.fileData, customConceptList=self.customConceptList, session=self.session)
            
    def getPersistent(self, cfvVO):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=cfvVO.value, origin=cfvVO.origin, fileDataOID=cfvVO.fileDataOID,
                                    customConcept=cfvVO.customConcept, endDate=cfvVO.endDate, periodOID=cfvVO.periodOID, session=self.session)
        return customFactValue
    
    def deleteImportedObject(self):
        CustomFactDao().deleteCFVByFD(self.fileData.OID, "COPY", self.session)
