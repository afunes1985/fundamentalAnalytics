'''
Created on Jun 29, 2019

@author: afunes
'''

from datetime import timedelta

import requests
from requests.exceptions import ReadTimeout

from dao.entityFactDao import EntityFactDao
from dao.fileDataDao import FileDataDao
from dao.priceDao import PriceDao
from importer.abstractImporter import AbstractImporter
from modelClass.price import Price
from valueobject.constant import Constant


class ImporterPrice(AbstractImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_PRICE, filename, replace, 'entityStatus', 'priceStatus', isNullPool=True)

    def doImport2(self):
#         try:
        conceptName = 'EntityCommonStockSharesOutstanding'
        entityFact = EntityFactDao().getEntityFact2(self.fileData.OID, conceptName, self.session)
        if entityFact is not None:
            self.periodOID = entityFact.periodOID
            dateToImportEnd = entityFact.period.getKeyDate()
        else:
            raise Exception("EntityFact not found")
        self.webSession = requests.Session()
        self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
        self.webSession.trust_env = False
        priceList = []
        daysToBack = 60
        if dateToImportEnd is not None:
            for ticker in self.fileData.company.tickerList:
                dateToImportStart = dateToImportEnd + timedelta(days=(daysToBack * -1))
                if(ticker.ticker is None):
                    raise Exception("Ticker not found")
                url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + ticker.ticker +'&interval=daily&start='+(dateToImportStart).strftime("%Y-%m-%d")+ '&end=' + (dateToImportEnd).strftime("%Y-%m-%d")
                response = self.webSession.get(url, timeout=2)
                r = response.json()
                if(r["history"] is not None and r["history"]["day"] is not None):
                    price = Price()
                    price.fileDataOID = self.fileData.OID
                    price.periodOID = self.periodOID
                    price.ticker = ticker
                    if(isinstance(r["history"]["day"], list)):
                        price.value =  r["history"]["day"][len(r["history"]["day"])-1]["close"]
                    elif(isinstance(r["history"]["day"], dict)):
                        price.value =  r["history"]["day"]["close"]
                    if (isinstance(price.value, float)):
                        priceList.append(price)
                if len(priceList) == 0:
                    raise Exception("DTB = "+ str(daysToBack) +" - Price not found for " + ticker.ticker +" Start=" + dateToImportStart.strftime("%Y-%m-%d") + " End=" + dateToImportEnd.strftime("%Y-%m-%d"))
#         except ReadTimeout:
#             FileDataDao().addOrModifyFileData(priceStatus = Constant.PRICE_STATUS_TIMEOUT, filename = self.filename, externalSession = self.session)
        return priceList
    
    def deleteImportedObject(self):
        PriceDao().deletePriceByFD(self.fileData.OID, self.session)

    def getPersistent(self, vo):
        return vo
        
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(priceStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey, externalSession = self.session)
          
    def addOrModifyFDError2(self, e):
        FileDataDao().addOrModifyFileData(priceStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey, externalSession = self.session)
