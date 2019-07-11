'''
Created on Jul 10, 2019

@author: afunes
'''
from importer.abstractImporter import AbstractImporter
from valueobject.constant import Constant
from engine.expressionEngine import ExpressionEngine


class ImporterExpression(AbstractImporter):

    def __init__(self, filename, replace, expressionDict):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_EXPRESSION, filename, replace)
        self.expressionDict = expressionDict
    
    def doImport2(self):
        cfVOList = ExpressionEngine().solveAndAddExpression(expressionDict = self.expressionDict, fileData = self.fileData, session = self.session)
        if(len(cfVOList) != 0):
            self.fileData.expressionStatus = Constant.STATUS_OK
        else: 
            self.fileData.expressionStatus = Constant.STATUS_NO_DATA
            
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(expressionStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey)         
       
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(expressionStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey)   
            
    def skipOrProcess(self):
        if((self.fileData.status == Constant.STATUS_OK and self.fileData.expressionStatus != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False   