'''
Created on 19 sep. 2018

@author: afunes
'''

from nt import listdir

import pandas
import xmltodict

from dao.factDao import FactDao
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from tools.tools import getXSDFileFromCache
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


class ImporterFact(AbstractImporter, AbstractFactImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_FACT, filename, replace, ConstantStatus.PRICE_STATUS, ConstantStatus.FACT_STATUS)
        self.processCache = None
            
    def doImport2(self):
        self.processCache = self.initProcessCache(self.filename, self.session)
        reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
        factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
        factVOList = self.setFactValues(factVOList, self.processCache)
        FactDao().addFact(factVOList, self.fileData, reportDict, self.session)
        return factVOList
    
    def getPersistentList(self, voList):
        return []    
    
    def deleteImportedObject(self):
        FactDao().deleteFactByFD(self.fileData.OID, self.session)
        
    def initCache(self):
        xsdCache = {}
        for xsdFileName in listdir(Constant.CACHE_FOLDER + "xsd"):
            try:
                xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, None)
                xsdDict = xmltodict.parse(xsdFile)
                xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
                xsdDF.set_index("@id", inplace=True)
                xsdDF.head()
                xsdCache[xsdFileName] = xsdDF
                print(xsdFileName)
            except Exception as e:
                self.logger.exception(e)
        AbstractImporter.cacheDict["XSD_CACHE"] = xsdCache
    