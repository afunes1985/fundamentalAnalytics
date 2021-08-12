'''
Created on Jul 10, 2019

@author: afunes
'''

from dao.customFactDao import CustomFactDao
from engine.customFactEngine import CustomFactEngine
from engine.expressionEngine import ExpressionEngine
from importer.abstractImporter import AbstractImporter
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterExpression(AbstractImporter):

    expressionDict = {}
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_EXPRESSION, filename, replace, ConstantStatus.COPY_STATUS, ConstantStatus.EXPRESSION_STATUS)
        self.expressionDict = ExpressionEngine().getExpressionDict(self.session)
    
    def doImport2(self):
        return ExpressionEngine().solveHistoricalExpression(expressionDict=self.expressionDict, fileData=self.fileData, session=self.session)
            
    def getPersistent(self, vo):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=vo.value, origin=vo.origin, fileDataOID=vo.fileDataOID,
                                    customConcept=vo.customConcept, periodOID=vo.periodOID, session=self.session)
        return customFactValue
    
    def deleteImportedObject(self):
        CustomFactDao().deleteCFVByFD(self.fileData.OID, "CALCULATED_BY_RULE", self.session)
