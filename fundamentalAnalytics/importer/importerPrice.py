'''
Created on Jun 29, 2019

@author: afunes
'''

from datetime import timedelta
import json

import requests
from requests.exceptions import ReadTimeout

from dao.entityFactDao import EntityFactDao
from dao.fileDataDao import FileDataDao
from dao.priceDao import PriceDao
from importer.abstractImporter import AbstractImporter
from modelClass.price import Price
from valueobject.constant import Constant
from tools.tools import EntityFactNotFoundException


class ImporterPrice(AbstractImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_PRICE, filename, replace, 'entityStatus', 'priceStatus', isNullPool=True)

    def doImport2(self):
#         try:
        conceptName = 'EntityCommonStockSharesOutstanding'
        entityFact = EntityFactDao().getFirstEntityFact(self.fileData.OID, conceptName, self.session)
        if entityFact is not None and entityFact.explicitMember.order_ != 99:
            self.periodOID = entityFact.periodOID
            dateToImportEnd = entityFact.period.getKeyDate()
        else:
            raise EntityFactNotFoundException("EntityFact not found " + entityFact.explicitMember.explicitMemberValue)
        self.webSession = requests.Session()
        self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
        self.webSession.trust_env = False
        priceList = []
        daysToBack = 0
        if dateToImportEnd is not None:
            for ticker in self.fileData.company.tickerList:
                dateToImportStart = dateToImportEnd + timedelta(days=(daysToBack * -1))
                if(ticker.ticker is None):
                    raise Exception("Ticker not found")
#                 url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + ticker.ticker +'&interval=daily&start='+(dateToImportStart).strftime("%Y-%m-%d")+ '&end=' + (dateToImportEnd).strftime("%Y-%m-%d")
#                 response = self.webSession.get(url, timeout=2)
#                 r = response.json()
#                 if(r["history"] is not None and r["history"]["day"] is not None):
#                     price = Price()
#                     price.fileDataOID = self.fileData.OID
#                     price.periodOID = self.periodOID
#                     price.ticker = ticker
#                     if(isinstance(r["history"]["day"], list)):
#                         price.value =  r["history"]["day"][len(r["history"]["day"])-1]["close"]
#                     elif(isinstance(r["history"]["day"], dict)):
#                         price.value =  r["history"]["day"]["close"]
#                     if (isinstance(price.value, float)):
#                         priceList.append(price)
                if len(priceList) == 0:
                    dateForUrl = dateToImportEnd.strftime('%Y%m%d')
                    #url = 'https://cloud.iexapis.com/stable/stock/' + ticker.ticker + '/chart/date/' + dateForUrl + '?chartByDay=true&token=pk_c4c339ea14ba4aad92d9256ac75705e4'
                    url = 'https://cloud.iexapis.com/stable/stock/' + ticker.ticker + '/chart/date/' + dateForUrl + '?chartByDay=true&token=pk_55cd20ce5c41439886a06ea27e1eb2e5'
                    result = requests.get(url)
                    if(result.ok):
                        json_data = json.loads(result.text)
                        if (len(json_data) > 0 and json_data[0]['date'] == dateToImportEnd.strftime('%Y-%m-%d')):
                            price = Price()
                            price.fileDataOID = self.fileData.OID
                            price.periodOID = self.periodOID
                            price.ticker = ticker
                            price.value = json_data[0]['close']
                            if (isinstance(price.value, float)
                                    or isinstance(price.value, int)):
                                priceList.append(price)
            if len(priceList) == 0:
                #raise Exception("DTB = "+ str(daysToBack) +" - Price not found for " + ticker.ticker +" Start=" + dateToImportStart.strftime("%Y-%m-%d") + " End=" + dateToImportEnd.strftime("%Y-%m-%d"))
                raise Exception("DTB = "+ str(daysToBack) +" - Price not found " + url)
#         except ReadTimeout:
#             FileDataDao().addOrModifyFileData(priceStatus = Constant.PRICE_STATUS_TIMEOUT, filename = self.filename, externalSession = self.session)
        return priceList
    
    def deleteImportedObject(self):
        PriceDao().deletePriceByFD(self.fileData.OID, self.session)

    def getPersistent(self, vo):
        return vo