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
        customFact.customFactValueList.extend(ExpressionEngine.solveExpression(ticker, customConceptName, customFact));
        Dao.addObject(objectToAdd = customFact, session = session, doCommit = True)
    
    @staticmethod
    def solveExpression(ticker, expressionName, customFact):
        expression = Dao.getExpression(expressionName)
        expr = parse_expr(expression.expression)
        valueByDateDict = {}
        symbolList = list(expr.free_symbols)
        periodsCompleted = [x.period.getKeyDate() for x in customFact.customFactValueList]
        for var in symbolList:
            cfvList = CustomFactDao.getCustomFactValue2(ticker, str(var))
            for cfv in cfvList:
                if (cfv.period.getKeyDate() not in periodsCompleted):
                    valueByDateDict.setdefault(cfv.period.getKeyDate(), {})[var] = cfv.value
                    valueByDateDict[cfv.period.getKeyDate()][cfv.period.type] = cfv.periodOID
        returnList = []
        for date, value in valueByDateDict.items():
            try:
                customFactValue = CustomFactValue()
                customFactValue.periodOID = value[expression.periodType]
                if(len(symbolList) == 2):
                    customFactValue.value = expr.subs([(symbolList[0], value[symbolList[0]]), (symbolList[1], value[symbolList[1]])])
                elif(len(symbolList) == 3):
                    customFactValue.value = expr.subs([(symbolList[0], value[symbolList[0]]), (symbolList[1], value[symbolList[1]]), (symbolList[2], value[symbolList[2]])])
                customFactValue.origin = 'CALCULATED_BY_RULE'
                returnList.append(customFactValue)
            except Exception as e:
                print("Error in periodDate " + str(date.strftime('%Y-%m-%d')) + " CustomFact missing -> " + str(e)) 
        print("Ready to add " + expressionName + " " + str(len(returnList)))
        return returnList
