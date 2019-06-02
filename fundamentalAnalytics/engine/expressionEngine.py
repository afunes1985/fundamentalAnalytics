'''
Created on 18 ago. 2018

@author: afunes
'''

from sympy.parsing.sympy_parser import parse_expr

from dao.customFactDao import CustomFactDao
from dao.dao import Dao
from modelClass.customFactValue import CustomFactValue


class ExpressionEngine(object):
    
    @staticmethod
    def solveCustomFactFromExpression(ticker, customConceptName, session = None):
        from engine.customFactEngine import CustomFactEngine
        customFact = CustomFactEngine.createCustomFact(ticker = ticker, customConceptName = customConceptName, session = session)
        customFact.customFactValueList = ExpressionEngine.solveExpression(ticker, customConceptName);
        Dao.addObject(objectToAdd = customFact, session = session, doCommit = True)
    
    @staticmethod
    def solveExpression(ticker, expressionName):
        expression = Dao.getExpression(expressionName)
        expr = parse_expr(expression.expression)
        periodDict = {}
        symbolList = list(expr.free_symbols)
        for var in symbolList:
            cfvList = CustomFactDao.getCustomFactValue(ticker, str(var), 'QTD')
            for cfv in cfvList:
                periodDict.setdefault(cfv.periodOID, {})[var] = cfv.value
                if(cfv.period.endDate is not None):
                    periodDict[cfv.periodOID]["DATE"] = cfv.period.endDate
                else:
                    periodDict[cfv.periodOID]["DATE"] = cfv.period.instant
        returnList = []
        for item, value in periodDict.items():
            try:
                customFactValue = CustomFactValue()
                customFactValue.periodOID = item
                if(len(symbolList) == 2):
                    customFactValue.value = expr.subs([(symbolList[0], value[symbolList[0]]), (symbolList[1], value[symbolList[1]])])
                elif(len(symbolList) == 3):
                    customFactValue.value = expr.subs([(symbolList[0], value[symbolList[0]]), (symbolList[1], value[symbolList[1]]), (symbolList[2], value[symbolList[2]])])
                customFactValue.origin = 'CALCULATED_BY_RULE'
                returnList.append(customFactValue)
            except Exception as e:
                print("Error in periodDate " + str(value["DATE"].strftime('%Y-%m-%d')) + " CustomFact missing -> " + str(e) + " periodOID " + str(item)) 
        return returnList
