'''
Created on Jun 29, 2019

@author: afunes
'''

import requests
from requests.exceptions import ReadTimeout

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.fileDataDao import FileDataDao
from importer.importer import AbstractImporter
from modelClass.price import Price
from valueobject.constant import Constant


class ImporterPrice(AbstractImporter):

    def __init__(self, ticker, filename, periodOID, dateToImport, fileDataOID, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_PRICE, filename, replace)
        self.ticker = ticker
        self.periodOID = periodOID
        self.dateToImport = dateToImport
        self.fileDataOID = fileDataOID

    def doImportPrice(self):
        try:
            self.webSession = requests.Session()
            self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
            self.webSession.trust_env = False
            self.session = DBConnector().getNewSession()
            url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + self.ticker +'&interval=daily&start='+(self.dateToImport).strftime("%Y-%m-%d")+ '&end=' + (self.dateToImport).strftime("%Y-%m-%d")
            response = self.webSession.get(url, timeout=2)
            r = response.json()
            if(r["history"] is not None):
                price = Price()
                price.fileDataOID = self.fileDataOID
                price.periodOID = self.periodOID
                price.value =  r["history"]["day"]["close"]
                if (isinstance(price.value, float)):
                    Dao().addObject(objectToAdd = price, doCommit = True, session = self.session)
                    self.fileData.priceStatus = Constant.STATUS_OK
                else:
                    self.fileData.priceStatus = Constant.STATUS_NO_DATA
            else:
                self.fileData.priceStatus = Constant.STATUS_NO_DATA
        except ReadTimeout:
            FileDataDao().addOrModifyFileData(priceStatus = Constant.PRICE_STATUS_TIMEOUT, filename = self.fileName)
        
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(priceStatus = Constant.STATUS_INIT, filename = self.filename, errorKey = self.errorKey)
          
    def addOrModifyFDError2(self, e):
        FileDataDao().addOrModifyFileData(priceStatus = Constant.STATUS_ERROR, filename = self.fileName, errorMessage = str(e)[0:190], errorKey = self.errorKey)
            
    def skipOrProcess(self):
        if((self.fileData.entityStatus == Constant.STATUS_OK and self.fileData.priceStatus != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False