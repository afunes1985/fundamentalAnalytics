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

    def __init__(self, entityFactValue, replace = False, session = None):
        self.session = DBConnector().getNewSession()
        self.replace = replace
        self.efv = entityFactValue
        self.webSession = requests.Session()
        self.webSession.headers.update({"Accept":"application/json","Authorization":"Bearer XGabnWN7VqBkIuSVvS6QrhwtiQcK"})
        self.webSession.trust_env = False


    def doImportPrice(self):
        start = time.time()
        try:
            ticker = self.efv.entityFact.company.ticker
            dateToImport = self.efv.period.getKeyDate() 
            url = 'https://sandbox.tradier.com/v1/markets/history?symbol=' + ticker +'&interval=daily&start='+(dateToImport).strftime("%Y-%m-%d")+ '&end=' + (dateToImport).strftime("%Y-%m-%d")
            response = self.webSession.get(url, timeout=2)
            r = response.json()
            print(r)
            if(r["history"] is not None):
                price = Price()
                price.fileDataOID = self.efv.entityFact.fileDataOID
                price.periodOID = self.efv.periodOID
                price.value =  r["history"]["day"]["close"]
                Dao().addObject(objectToAdd = price, doFlush = True, session = self.session)
                self.efv.entityFact.fileData.priceStatus = "OK"
                Dao().addObject(objectToAdd = self.efv.entityFact.fileData, doCommit = True, session = self.session)
        except Exception as e:
            print(str(e))
            FileDataDao().addOrModifyFileData("ERROR", priceStatus = "ERROR", filename = self.efv.entityFact.fileData.fileName)
        finally:
            self.session.close()
        end = time.time()
        print('Time: {}'.format(end - start))