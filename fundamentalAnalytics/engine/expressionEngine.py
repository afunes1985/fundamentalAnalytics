'''
Created on 18 ago. 2018

@author: afunes
'''

from dao.dao import Dao
from modelClass.customFactValue import CustomFactValue
from sympy.parsing.sympy_parser import parse_expr


class ExpressionEngine(object):
    @staticmethod
    def solveExpression(ticker, expressionName):
        expression = Dao.getExpression(expressionName)
        expr = parse_expr(expression.expression)
        periodDict = {}
        symbolList = list(expr.free_symbols)
        for var in symbolList:
            itemList = Dao.getCustomFactValue(ticker, str(var), 'QTD')
            for item in itemList:
                periodDict.setdefault(item.periodOID, {})[var] = item.value
                
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
                print("Error in period " + str(item) + " " + str(e))
        return returnList
