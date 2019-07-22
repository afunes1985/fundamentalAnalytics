'''
Created on Jun 29, 2019

@author: afunes
'''

import requests
from requests.exceptions import ReadTimeout

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.fileDataDao import FileDataDao
from importer.abstractImporter import AbstractImporter
from modelClass.price import Price
from valueobject.constant import Constant


class ImporterPrice(AbstractImporter):

    def __init__(self, ticker, filename, periodOID, dateToImport, fileDataOID, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_PRICE, filename, replace, 'entityStatus', 'priceStatus')
        self.ticker = ticker
        self.periodOID = periodOID
        self.dateToImport = dateToImport
        self.fileDataOID = fileDataOID

    def doImport2(self):
        try:
            self.webSession = requests.Session()
            self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
            self.webSession.trust_env = False
            url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + self.ticker +'&interval=daily&start='+(self.dateToImport).strftime("%Y-%m-%d")+ '&end=' + (self.dateToImport).strftime("%Y-%m-%d")
            response = self.webSession.get(url, timeout=2)
            r = response.json()
            priceList = []
            if(r["history"] is not None):
                price = Price()
                price.fileDataOID = self.fileDataOID
                price.periodOID = self.periodOID
                price.value =  r["history"]["day"]["close"]
                if (isinstance(price.value, float)):
                    priceList.append(price)
        except ReadTimeout:
            FileDataDao().addOrModifyFileData(priceStatus = Constant.PRICE_STATUS_TIMEOUT, filename = self.fileName, externalSession = self.session)
        return priceList
    
    def deleteImportedObject2(self):
        self.deleteImportedObject(self.fileData.priceList)
        self.fileData.priceList = []

    def getPersistent(self, vo):
        return vo
        
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(priceStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey, externalSession = self.session)
          
    def addOrModifyFDError2(self, e):
        FileDataDao().addOrModifyFileData(priceStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:149], errorKey = self.errorKey, externalSession = self.session)
