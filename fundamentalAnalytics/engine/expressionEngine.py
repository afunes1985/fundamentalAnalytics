'''
Created on 18 ago. 2018

@author: afunes
'''

from sympy.parsing.sympy_parser import parse_expr

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao
from dao.expressionDao import ExpressionDao
from dao.fileDataDao import FileDataDao
from pricingAPI.PricingInterface import PricingInterface
from valueobject.valueobject import CustomFactValueVO
from dao.companyDao import CompanyDao


class ExpressionEngine(object):

    
    def solveHistoricalExpression(self, expressionDict, fileData, session):
        cfvDict = self.getValuesForExpression(fileDataOID=fileData.OID, isCurrent=None, session=session)
        return self.solveExpression(expressionDict, fileData, cfvDict)
    
    def solveCurrentExpression(self, CIK, ticker, session):
        rs = FileDataDao().getLastFileDataByCIK(CIK=CIK, session=session)
        row = rs.fetchone()
        tickerObject = CompanyDao().getTicker(ticker, session)
        fileData = FileDataDao().getFileData(row[0], session)
        expressionDict = self.getExpressionDict2(isCurrent=True, session=session)
        cfvDict = self.getValuesForExpression(fileDataOID=fileData.OID, isCurrent=True, session=session)
        priceValue = PricingInterface().getMarketPriceByAssetName(tickerObject.onlinePricingSymbol, PricingInterface.TRADIER)
        cfvDict['PRICE'] = {'value': priceValue, 'periodOID': None}
#         print('PRICE', priceValue)
        return self.solveExpression(expressionDict, fileData, cfvDict)
    
    def getExpressionDict(self, session):
        expressionList = ExpressionDao().getExpressionList(session=session)
        return self.convertListToDictExp(expressionList)
    
    def getExpressionDict2(self, isCurrent, session):
        expressionList = ExpressionDao().getExpressionList2(isCurrent=isCurrent, session=session)
        return self.convertListToDictExp(expressionList)
       
    def getValuesForExpression(self, fileDataOID, isCurrent, session):
        if(isCurrent):
            rs = Dao().getValuesForCurrentExpression(fileDataOID, session)
        else:
            rs = Dao().getValuesForExpression(fileDataOID, session)
        return self.convertRStoDict(rs)
        
    def convertListToDictExp(self, expressionList):
        expressionDict = {}
        for expression in expressionList:
            expr = parse_expr(expression.expression)
            expressionDict[expression] = expr
        return expressionDict
        
    def convertRStoDict(self, rs):
        cfvDict = {}
        for row in rs:
            if cfvDict.get(row.conceptName, None) is None:
                cfvDict[row.conceptName] = dict(row.items())
            else:
                raise Exception("Duplicated values " + row.conceptName)
        return cfvDict
    
    def solveExpression(self, expressionDict, fileData, cfvDict):
        returnList = []
        errorList = []
        periodDict ={}
        for expression, expr in expressionDict.items():
            if (expression.customConcept.conceptName not in cfvDict.keys()):
                symbolList = list(expr.free_symbols)
                symbolList = [str(x) for x in symbolList]
                try:
                    periodOID = periodDict.get(expression.customConcept.periodType, None)
                    for symbol in symbolList:
                        if(periodOID is None):
                            periodOID = cfvDict[symbol]["periodOID"]
                    if(periodOID is None):
                        print(cfvDict)
                        raise Exception("Period OID not found for " + symbol + " " + str(expr))
                    else:
                        firstPeriodOID = periodDict.get(expression.customConcept.periodType, None)
                        if(firstPeriodOID is None):
                            periodDict[expression.customConcept.periodType] = periodOID
                        elif(firstPeriodOID != periodOID):
                            raise Exception("More than one period for periodType " + str(periodOID) + str(firstPeriodOID))
                    if(len(symbolList) == 2):
                        value = expr.subs([(symbolList[0], cfvDict[symbolList[0]]["value"]), (symbolList[1], cfvDict[symbolList[1]]["value"])])
                        print(expression.customConcept.conceptName, expr, symbolList[0], cfvDict[symbolList[0]]["value"], symbolList[1], cfvDict[symbolList[1]]["value"])
                    elif(len(symbolList) == 3):
                        value = expr.subs([(symbolList[0], cfvDict[symbolList[0]]["value"]), (symbolList[1], cfvDict[symbolList[1]]["value"]), (symbolList[2], cfvDict[symbolList[2]]["value"])])
                    origin = 'CALCULATED_BY_RULE'
                    cfv = CustomFactValueVO(value=value, origin=origin, fileDataOID=fileData.OID, customConcept=expression.customConcept, 
                                            order_=expression.customConcept.defaultOrder, periodOID=periodOID, periodType=expression.customConcept.periodType)
                    cfvDict[expression.customConcept.conceptName] = {"value": value, "periodOID" : periodOID}
                    returnList.append(cfv)
                except KeyError as e:
                    errorList.append(expression.customConcept.conceptName + " fail for " + str(e)) 
        #print("Ready to add CFV from Expressions " + str(len(returnList)))
        return returnList
    

if __name__ == '__main__':
    Initializer()
    session = DBConnector(isNullPool=True).getNewSession()
#     fileData = FileDataDao.getFileData('edgar/data/320193/0000320193-21-000010.txt', session)
    ee = ExpressionEngine()
    rList = ee.solveCurrentExpression(CIK = '858877', ticker='CSCO', session=session)
    for cfVO in rList:
        print(cfVO.customConcept.conceptName, cfVO.value)
#     priceValue = PricingInterfaceTradier().getMarketPriceByAssetName('AAPL')
#     print(priceValue)
