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

    def __init__(self, filename, replace, ticker = None, periodOID = None, dateToImport = None, fileDataOID = None):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_PRICE, filename, replace, 'entityStatus', 'priceStatus')
        self.dateToImport = None
        if (ticker is None):
            fileData = FileDataDao().getFileData(filename, self.session)
            self.ticker = fileData.company.ticker
        else:
            self.ticker = ticker
        if(fileDataOID is None):
            self.fileDataOID = fileData.OID
        else:
            self.fileDataOID = fileDataOID
        self.periodOID = periodOID

    def doImport2(self):
        try:
            if(self.periodOID is None):
                conceptName = 'EntityCommonStockSharesOutstanding'
                entityFact = EntityFactDao().getEntityFact2(self.fileDataOID, conceptName, self.session)
                if entityFact is not None:
                    self.periodOID = entityFact.periodOID
                    self.dateToImport = entityFact.period.getKeyDate()
                else:
                    raise Exception("EntityFact not found")
            else:
                self.periodOID = self.periodOID
                self.dateToImport = self.periodOID
            self.webSession = requests.Session()
            self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
            self.webSession.trust_env = False
            priceList = []
            for i in range(0,5):
                if self.dateToImport is not None:
                    self.dateToImport = self.dateToImport + timedelta(days=(i*-1))
                    url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + self.ticker +'&interval=daily&start='+(self.dateToImport).strftime("%Y-%m-%d")+ '&end=' + (self.dateToImport).strftime("%Y-%m-%d")
                    response = self.webSession.get(url, timeout=2)
                    r = response.json()
                    if(r["history"] is not None):
                        price = Price()
                        price.fileDataOID = self.fileDataOID
                        price.periodOID = self.periodOID
                        price.value =  r["history"]["day"]["close"]
                        if (isinstance(price.value, float)):
                            priceList.append(price)
                            break
            if len(priceList) == 0:
                raise Exception("Price not found for " + self.ticker + " " + self.dateToImport.strftime("%Y-%m-%d"))
        except ReadTimeout:
            FileDataDao().addOrModifyFileData(priceStatus = Constant.PRICE_STATUS_TIMEOUT, filename = self.filename, externalSession = self.session)
        return priceList
    
    def deleteImportedObject(self):
        PriceDao().deletePriceByFD(self.fileData.OID, self.session)

    def getPersistent(self, vo):
        return vo
        
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(priceStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey, externalSession = self.session)
          
    def addOrModifyFDError2(self, e):
        FileDataDao().addOrModifyFileData(priceStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey, externalSession = self.session)
