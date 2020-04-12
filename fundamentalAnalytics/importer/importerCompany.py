'''
Created on 19 sep. 2018

@author: afunes
'''

from datetime import datetime
import json
from nt import listdir

import pandas
import requests
import xmltodict

from dao.dao import Dao
from engine.companyEngine import CompanyEngine
from engine.periodEngine import PeriodEngine
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from tools.tools import getXSDFileFromCache, LastPriceNotFound, \
    LastPriceIsTooOld
from valueobject.constant import Constant


class ImporterCompany(AbstractImporter, AbstractFactImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_COMPANY, filename, replace, 'fileStatus', 'companyStatus')
        self.processCache = None
            
    def doImport2(self):
        self.priceValidated = False
        self.validateLastPrice()
        self.processCache = self.initProcessCache2(self.filename, self.session)
        company = self.fillCompanyData(self.session)
        self.fileData.company = company
        self.validateLastPrice()
            
    def getPersistentList(self, voList):
        return []    
    
    def deleteImportedObject(self):
        pass
       
    def initCache(self):
        pass
#         xsdCache = {}
#         for xsdFileName in listdir(Constant.CACHE_FOLDER + "xsd"):
#             try:
#                 xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, None)
#                 xsdDict = xmltodict.parse(xsdFile)
#                 xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
#                 xsdDF.set_index("@id", inplace=True)
#                 xsdDF.head()
#                 xsdCache[xsdFileName] = xsdDF
#                 print(xsdFileName)
#             except Exception as e:
#                 self.logger.exception(e)
#         AbstractImporter.cacheDict["XSD_CACHE"] = xsdCache

    def validateLastPrice(self):
        if(self.fileData.company is not None and self.fileData.company.listed and not self.priceValidated):
            atLeastOnePriceFound = False
            atLeastOnePriceIsNew = False
            resultList = []
            for ticker in self.fileData.company.tickerList:
#                result = requests.get('https://cloud.iexapis.com/stable/stock/' + ticker.ticker + '/quote?token=pk_c4c339ea14ba4aad92d9256ac75705e4')
                result = requests.get('https://cloud.iexapis.com/stable/stock/' + ticker.ticker + '/quote?token=pk_55cd20ce5c41439886a06ea27e1eb2e5')
                resultList.append(result)
                if(result.ok):
                    atLeastOnePriceFound = True
                    ticker.active = 1 #if price is found, enable ticker
                elif(result.status_code == 402):
                    raise Exception("Error in price provider")
                else:
                    ticker.active = 0 #if price doesn't found, disable ticker
            if(not atLeastOnePriceFound):
                self.fileData.company.listed = False
                self.fileData.company.notListedDescription = Constant.STATUS_LAST_PRICE_NF
                raise LastPriceNotFound()
            for result in resultList:
                if(result.ok):
                    json_data = json.loads(result.text)
                    lastestTime = json_data['latestTime']
                    try:
                        if(lastestTime != 'N/A'):
                            datetime.strptime(lastestTime, '%H:%M:%S %p')
                            atLeastOnePriceIsNew = True
                            break
                    except (ValueError):
                        dateTime = datetime.strptime(lastestTime, '%B %d, %Y')
                        if(PeriodEngine().getDaysBetweenDates(dateTime, datetime.now()) > 30):
                            atLeastOnePriceIsNew = False
                        else:
                            atLeastOnePriceIsNew = True
                            break
            if(not atLeastOnePriceIsNew):
                self.fileData.company.listed = False
                self.fileData.company.notListedDescription = Constant.STATUS_LAST_PRICE_IS_TOO_OLD
                raise LastPriceIsTooOld()
            self.priceValidated = True