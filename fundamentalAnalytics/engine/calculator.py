'''
Created on 18 ago. 2018

@author: afunes
'''
from dao.dao import DaoCompanyResult


class Calculator(object):


    def __init__(self, params):
        '''
        Constructor
        '''
    @staticmethod
    def calculateBookValue(ticker):
        rsStockHoldersEquity = DaoCompanyResult.getCompanyResult(None, ticker, "StockholdersEquity");
        rsOutstandingShares = DaoCompanyResult.getCompanyResult(None, ticker, "CommonStockSharesOutstanding");
        rowsStockHoldersEquity = rsStockHoldersEquity.fetchall()
        rowsOutstandingShares = rsOutstandingShares.fetchall()
        resultDict = {"indicatorID" : "bookValuePerShare",
                      "x" : [],
                      "y" : []}
        if (len(rowsStockHoldersEquity) != 0): 
            for index, rowStockHoldersEquity in enumerate(rowsStockHoldersEquity):
                if rowStockHoldersEquity is not None:
                    stockHoldersEquity = rowStockHoldersEquity[1]
                    outstandingShares = rowsOutstandingShares[index][1]
                    if(stockHoldersEquity == 0 and stockHoldersEquity is None):
                        bookValuePerShare = 'ERROR stockHoldersEquity = 0 or None'
                    elif(outstandingShares == 0 and outstandingShares is None):
                        bookValuePerShare = 'ERROR outstandingShares = 0 or None'   
                    elif(rowStockHoldersEquity[0] != rowsOutstandingShares[index][0]):
                        bookValuePerShare = 'ERROR not same period'   
                    else: 
                        bookValuePerShare = stockHoldersEquity / outstandingShares
                        resultDict["y"].append(bookValuePerShare)
                        resultDict["x"].append(rowStockHoldersEquity[0] )
                        print (rowStockHoldersEquity[0] + " book Value Per Share " + str(bookValuePerShare) + " oustanding shares " + str(outstandingShares) + " StockHoldersEquity " + str(stockHoldersEquity) )
        return resultDict            