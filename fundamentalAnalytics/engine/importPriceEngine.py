'''
Created on Jun 29, 2019

@author: afunes
'''
import time

import requests

from base.dbConnector import DBConnector
from dao.dao import Dao
from modelClass.price import Price
from dao.fileDataDao import FileDataDao


class ImportPriceEngine():

    def __init__(self, ticker, fileName, periodOID, dateToImport, fileDataOID):
        self.ticker = ticker
        self.fileName = fileName
        self.periodOID = periodOID
        self.dateToImport = dateToImport
        self.fileDataOID = fileDataOID

    def doImportPrice(self):
        start = time.time()
        try:
            self.webSession = requests.Session()
            self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
            self.webSession.trust_env = False
            self.session = DBConnector().getNewSession()
            url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + self.ticker +'&interval=daily&start='+(self.dateToImport).strftime("%Y-%m-%d")+ '&end=' + (self.dateToImport).strftime("%Y-%m-%d")
            response = self.webSession.get(url, timeout=2)
            r = response.json()
            print(r)
            if(r["history"] is not None):
                price = Price()
                price.fileDataOID = self.fileDataOID
                price.periodOID = self.periodOID
                price.value =  r["history"]["day"]["close"]
                Dao().addObject(objectToAdd = price, doCommit = True, session = self.session)
                FileDataDao().addOrModifyFileData(priceStatus = "OK", filename = self.fileName)
            else:
                FileDataDao().addOrModifyFileData(priceStatus = "NO_DATA", filename = self.fileName)
        except Exception as e:
            print(str(e))
            FileDataDao().addOrModifyFileData(priceStatus = "ERROR", filename = self.fileName)
        finally:
            self.session.close()
        end = time.time()
        print('Time: {}'.format(end - start))