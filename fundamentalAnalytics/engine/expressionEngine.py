'''
Created on 18 ago. 2018

@author: afunes
'''

from dao.dao import Dao
from engine.customFactEngine import CustomFactEngine
from modelClass.customFactValue import CustomFactValue
from valueobject.valueobject import CustomFactValueVO


class ExpressionEngine(object):
    
    def solveAndAddExpression(self, expressionDict, fileData, session):
        cfvList = self.solveExpression(expressionDict, fileData, session)
        return cfvList
    
    def solveExpression(self, expressionDict, fileData, session):
        returnList = []
        cfvDict = {}
        errorList = []
        rs = Dao().getValuesForExpression(fileData.OID, session)
        for row in rs:
            cfvDict[row.conceptName] = dict(row.items())
                
        for expression, expr in expressionDict.items():
            if (expression.customConcept.conceptName not in cfvDict.keys()):
                symbolList = list(expr.free_symbols)
                symbolList = [str(x) for x in symbolList]
                try:
                    periodOID = None
                    for symbol in symbolList:
                        if(periodOID is None):
                            periodOID = cfvDict[symbol]["periodOID"]
                    if(len(symbolList) == 2):
                        value = expr.subs([(symbolList[0], cfvDict[symbolList[0]]["value"]), (symbolList[1], cfvDict[symbolList[1]]["value"])])
                    elif(len(symbolList) == 3):
                        value = expr.subs([(symbolList[0], cfvDict[symbolList[0]]["value"]), (symbolList[1], cfvDict[symbolList[1]]["value"]), (symbolList[2], cfvDict[symbolList[2]]["value"])])
                    origin = 'CALCULATED_BY_RULE'
                    fileDataOID = fileData.OID
                    cfv = CustomFactValueVO(value, origin, fileDataOID, expression.customConcept, expression.customConcept.defaultOrder, periodOID)
                    cfvDict[expression.customConcept.conceptName] = {"value": value, "periodOID" : periodOID}
                    returnList.append(cfv)
                except KeyError as e:
                    errorList.append(expression.customConcept.conceptName + " fail for " + str(e)) 
        #print("Ready to add CFV from Expressions " + str(len(returnList)))
        return returnList
