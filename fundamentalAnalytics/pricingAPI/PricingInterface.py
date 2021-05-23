'''
Created on 8 nov. 2017

@author: afunes
'''
from datetime import datetime, timedelta
from decimal import Decimal
import http
import json
import logging

import requests

from dao.priceDao import PriceDao


class PricingInterface:
    @staticmethod
    def getPriceInterfacesDict():
        return dict({"EXCEL":PricingInterfaceExcel(), 
                    #"YAHOO":PricingInterfaceYahoo,
                    "TRADIER":PricingInterfaceTradier(),
                    "ALPHAVANTAGE":PricingInterfaceAlphaVantage(),
                    "DB":PricingInterfaceDB(),
                    "IEX":PricingInterfaceIEX()}) 
        
    @staticmethod
    def getExchangeRateByCurrency(fromCurrency, toCurrency):
        try:
            return PricingInterface.getPriceInterfacesDict()["ALPHAVANTAGE"].getExchangeRateByCurrency(fromCurrency, toCurrency)
        except Exception as e:
            logging.exception(e)
            return 0
        
    @staticmethod
    def getMarketPriceByAssetName(assetName, priceSource):
        try:
            return PricingInterface.getPriceInterfacesDict()[priceSource].getMarketPriceByAssetName(assetName)
        except Exception as e:
            logging.warning("Price not found "+ assetName + " " + priceSource)
            return 0
        
    @staticmethod
    def getReferenceDataByAssetNames(assetNames, priceSource):
        try:    
            return PricingInterface.getPriceInterfacesDict()[priceSource].getReferenceDataByAssetNames(assetNames)
        except Exception as e:
            logging.warning(e)
            return []
    
    @staticmethod
    def getMarketPriceByDate(assetName, priceSource, date):
        try:    
            return PricingInterface.getPriceInterfacesDict()[priceSource].getMarketPriceByDate(assetName, date)
        except Exception as e:
            logging.warning(e)
            return []
        
    @staticmethod
    def getExchangeRateByDate(fromCurrency, toCurrency, date):  
        return PricingInterface.getPriceInterfacesDict()["IEX"].getExchangeRateByDate(fromCurrency, toCurrency, date)


########################################################################################################################

class PricingInterfaceAlphaVantage:
    def getExchangeRateByCurrency(self, fromCurrency, toCurrency):
        result = requests.get('https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency='+fromCurrency+'&to_currency='+toCurrency+'&apikey=Z09WI322376KBA3P')
        json_data = json.loads(result.text)
        return Decimal(json_data['Realtime Currency Exchange Rate']['5. Exchange Rate'])

########################################################################################################################

class PricingInterfaceIEX():    
    def getMarketPriceByDate(self, assetName, date):
        daysToBack = 10
        for offset in range(0, daysToBack):
            dateToImport = date + timedelta(days=(offset * -1))
            dateForUrl = dateToImport.strftime('%Y%m%d')
            url = 'https://cloud.iexapis.com/stable/stock/' + assetName + '/chart/date/' + dateForUrl + '?chartByDay=true&token=pk_55cd20ce5c41439886a06ea27e1eb2e5'
            result = requests.get(url)
            if(result.ok):
                json_data = json.loads(result.text)
                if (len(json_data) > 0 and json_data[0]['date'] == date.strftime('%Y-%m-%d')):
                    return json_data[0]['close']
    
    def getExchangeRateByDate(self, fromCurrency, toCurrency, date):   
        """The requested data is not available to free tier accounts. Please upgrade for access to this data."""
        dateForUrl = date.strftime('YYYYMMDD')
        url = 'https://cloud.iexapis.com/stable/fx/historical?symbols=MXNUSD&from=' + dateForUrl + '&token=pk_55cd20ce5c41439886a06ea27e1eb2e5'
        result = requests.get(url)
        print(result)
        json_data = json.loads(result.text)
        print(json_data) 
    
########################################################################################################################
    
class PricingInterfaceTradier:
    def getMarketPriceByAssetName(self, assetName):
        # Request: Market Quotes (https://sandbox.tradier.com/v1/markets/quotes?symbols=spy)
        connection = http.client.HTTPSConnection('sandbox.tradier.com', 443, timeout = 30)  # @UndefinedVariable
        # Headers
        headers = {"Accept":"application/json",
                   "Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"}
        # Send synchronously
        connection.request('GET', '/v1/markets/quotes?symbols='+assetName, None, headers)
        response = connection.getresponse()
        content = response.read()
        json_data = json.loads(content)
        return Decimal(json_data['quotes']['quote']['last'])
    
    def getReferenceDataByAssetNames(self, assetNames):
        returnList = []
        # Request: Market Quotes (https://sandbox.tradier.com/v1/markets/quotes?symbols=spy)
        connection = http.client.HTTPSConnection('sandbox.tradier.com', 443, timeout = 30)  # @UndefinedVariable
        # Headers
        headers = {"Accept":"application/json",
                   "Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"}
        # Send synchronously
        connection.request('GET', '/v1/markets/quotes?symbols='+assetNames, None, headers)
        response = connection.getresponse()
        content = response.read()
        json_data = json.loads(content)
        if isinstance(json_data['quotes']['quote'], list): 
            for row in json_data['quotes']['quote']:
                returnRow = []
                returnRow.append(row['symbol'])
                returnRow.append(row['last'])
                returnRow.append(row['change_percentage'])
                returnList.append(returnRow)
        else:
            returnRow = []
            returnRow.append(json_data['quotes']['quote']['symbol'])
            returnRow.append(json_data['quotes']['quote']['last'])
            returnRow.append(json_data['quotes']['quote']['change_percentage'])
            returnList.append(returnRow)
        return returnList
    
    @staticmethod
    def getHistoricalPrice(assetName, fromDate, ToDate):
        return None

########################################################################################################################
    
class PricingInterfaceExcel:
    def getExchangeRateByCurrency(self, fromCurrency, toCurrency):
        import pandas
        df = pandas.read_csv('C://Users//afunes//iCloudDrive//PortfolioViewer//import//quotes.csv');
        symbolValues = df['Symbol'].values
        curPriceValues = df['Current Price'].values
        for index, rfRow in enumerate(symbolValues):
            if 'MXN=X' == rfRow:
                currentPrice = curPriceValues[index]
                #change = changeValues[index]
                #returnRow.append(round((((currentPrice)/(currentPrice-change)-1)*100), 2))
                return round(currentPrice,2)
            
    def getExchangeRateByDate(self, fromCurrency, toCurrency, date):
        import pandas
        currencyName = fromCurrency+"/"+toCurrency
        df = pandas.read_excel('C://Users//afunes//iCloudDrive//PortfolioViewer//import//ImportHistoricalPriceAndExchangeRate.xlsx')
        for index, row in df.iterrows():
            if(currencyName == row['Asset Name']
                and date == datetime.strptime(str(row['Date']), '%d-%m-%Y').date()):
                closePrice = row['Close*']
                return round(closePrice,2)
            
    def getMarketPriceByDate(self, assetName, date):
        import pandas
        df = pandas.read_excel('C://Users//afunes//iCloudDrive//PortfolioViewer//import//ImportHistoricalPriceAndExchangeRate.xlsx')
        for index, row in df.iterrows():
            if(assetName == row['Asset Name']
                and date == datetime.strptime(str(row['Date']), '%d-%m-%Y').date()):
                closePrice = row['Close*']
                return round(closePrice,2)
    
    def getMarketPriceByAssetName(self, assetName):
        return 0
    
    def getReferenceDataByAssetNames(self, assetNames):
        import pandas
        df = pandas.read_csv('C://Users//afunes//iCloudDrive//PortfolioViewer//import//quotes.csv');
        symbolValues = df['Symbol'].values
        curPriceValues = df['Current Price'].values
        changeValues = df['Change'].values
        returnList = []
        for index, rfRow in enumerate(symbolValues):
            if assetNames == rfRow:
                returnRow = []
                returnRow.append(rfRow)
                currentPrice = curPriceValues[index]
                returnRow.append(round(currentPrice,2))
                change = changeValues[index]
                returnRow.append(round((((currentPrice)/(currentPrice-change)-1)*100), 2))
                returnList.append(returnRow)
        return returnList

########################################################################################################################

class PricingInterfaceDB:
    def getMarketPriceByAssetName(self, assetName):
        return 0
    
    def getReferenceDataByAssetNames(self, assetNames):
        lastPrice = PriceDao().getLastPrice(assetNames)
        returnList = []
        returnRow = []
        returnRow.append(assetNames)
        currentPrice = lastPrice[0]
        returnRow.append(round(currentPrice,2))
        change = 0
        returnRow.append(round((((currentPrice)/(currentPrice-change)-1)*100), 2))
        returnList.append(returnRow)
        return returnList

if __name__ == '__main__':
    datetime.strptime('Dec 31, 2015', '%b %d, %Y')
    
    cp = PricingInterface.getExchangeRateByDate("USD", "MXN", datetime(2020, 12, 31).date())
#     cp = PricingInterfaceExcel().getExchangeRateByDate("USD", "MXN", datetime(2019, 12, 31).date())
#     cp = PricingInterfaceExcel().getPriceByDate(assetName='ICA.MX', date=datetime(2019, 12, 31).date())
    print(cp)
