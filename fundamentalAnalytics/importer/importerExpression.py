'''
Created on Jul 10, 2019

@author: afunes
'''
from sympy.parsing.sympy_parser import parse_expr

from dao.customFactDao import CustomFactDao
from dao.expressionDao import ExpressionDao
from engine.customFactEngine import CustomFactEngine
from engine.expressionEngine import ExpressionEngine
from importer.abstractImporter import AbstractImporter
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterExpression(AbstractImporter):

    expressionDict = {}
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_EXPRESSION, filename, replace, ConstantStatus.COPY_STATUS, ConstantStatus.EXPRESSION_STATUS)
        expressionList = ExpressionDao().getExpressionList(session=self.session)
        self.expressionDict = {}
        for expression in expressionList:
            expr = parse_expr(expression.expression)
            self.expressionDict[expression] = expr
    
    def doImport2(self):
        return ExpressionEngine().solveAndAddExpression(expressionDict=self.expressionDict, fileData=self.fileData, session=self.session)
            
    def getPersistent(self, vo):
        customFactValue = CustomFactEngine().getNewCustomFactValue(value=vo.value, origin=vo.origin, fileDataOID=vo.fileDataOID,
                                    customConcept=vo.customConcept, periodOID=vo.periodOID, session=self.session)
        return customFactValue
    
    def deleteImportedObject(self):
        CustomFactDao().deleteCFVByFD(self.fileData.OID, "CALCULATED_BY_RULE", self.session)
