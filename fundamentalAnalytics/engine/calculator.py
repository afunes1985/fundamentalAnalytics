'''
Created on 18 ago. 2018

@author: afunes
'''
from sympy.core.symbol import symbols
from sympy.solvers.solveset import linsolve

from dao.dao import Dao
from modelClass.customFactValue import CustomFactValue
from sympy.parsing.sympy_parser import parse_expr


class Calculator(object):

    
    @staticmethod
    def solveRule(ticker, expressionName):
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
                customFactValue.value = expr.subs([(symbolList[0], value[symbolList[0]]), (symbolList[1], value[symbolList[1]])])
                returnList.append(customFactValue)
            except Exception as e:
                print("Error in period " + str(item) + " " + str(e))
        return returnList
