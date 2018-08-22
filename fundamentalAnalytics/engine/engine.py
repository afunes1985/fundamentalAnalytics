'''
Created on 18 ago. 2018

@author: afunes
'''
from dao.dao import DaoCompanyFundamental


class Engine(object):


    def __init__(self, params):
        '''
        Constructor
        '''
    @staticmethod
    def calculateBookValue(companyID):
        #LiabilitiesCurrent, CommonStockSharesOutstanding, Assets
        #rsDictAsset = DaoCompanyFundamental.getCompanyFundamental(companyID, "Assets");
        rsDictStockHoldersEquity = DaoCompanyFundamental.getCompanyFundamental(companyID, "StockholdersEquity");
        rsDictOutstandingShares = DaoCompanyFundamental.getCompanyFundamental(companyID, "CommonStockSharesIssued");
        #rsDictLiabilities = DaoCompanyFundamental.getCompanyFundamental(companyID, "Liabilities");
        if (len(rsDictStockHoldersEquity["rs"]) != 0): 
            for index, asset in enumerate(rsDictStockHoldersEquity["rs"][0]):
                if asset is not None:
                    print(rsDictStockHoldersEquity["column"][index])
                    StockHoldersEquity = asset
                    print("StockHoldersEquit " + str(StockHoldersEquity/1000))
                    #liabilitie = rsDictLiabilities["rs"][0][index]
                    #print ("liabilitie " + str(liabilitie/1000))
                    #bookValue = asset - liabilitie
                    #print ("book value " + str(bookValue/1000))
                    outstandingShares = rsDictOutstandingShares["rs"][0][index]
                    print ("oustanding shares " + str(outstandingShares))
                    bookValuePerShare = StockHoldersEquity / outstandingShares
                    print ("book Value Per Share " + str(bookValuePerShare))