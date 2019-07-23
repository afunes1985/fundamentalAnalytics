'''
Created on 19 sep. 2018

@author: afunes
'''
import logging
from nt import listdir

import pandas
import xmltodict

from dao.dao import Dao
from dao.entityFactDao import EntityFactDao
from engine.companyEngine import CompanyEngine
from engine.periodEngine import PeriodEngine
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from tools.tools import getXMLDictFromGZCache, getXSDFileFromCache
from valueobject.constant import Constant
from valueobject.valueobject import FactVO


class ImporterEntityFact(AbstractImporter, AbstractFactImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_ENTITY_FACT, filename, replace, 'status', 'entityStatus')
        self.processCache = None
        self.conceptName = 'EntityCommonStockSharesOutstanding'
            
    def doImport2(self):
        self.processCache = self.initProcessCache(self.filename, self.session)
        reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
        factVOList = self.getFactByConcept(reportDict, self.processCache, self.conceptName)
        factVOList = self.setFactValues(factVOList, self.processCache)
        EntityFactDao().addEntityFact(factVOList, self.fileData.OID , reportDict, self.session, self.replace)
        return factVOList
            
    def addOrModifyFDError1(self, e):
        self.fileDataDao.addOrModifyFileData(entityStatus=e.status, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey, externalSession=self.session)
    
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_ERROR, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey, externalSession=self.session)         
            
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_INIT, priceStatus=Constant.STATUS_PENDING, filename=self.filename, errorKey=self.errorKey, externalSession=self.session)   
            
    def getPersistentList(self, voList):
        return []       
    
    def deleteImportedObject(self):
        EntityFactDao().deleteEFVByFD(self.fileData.OID, self.session)
            
    def getPeriodDict(self, xmlDictRoot, session):
        periodDict = {}
        for item in self.getListFromElement(Constant.XBRL_CONTEXT, xmlDictRoot):
            entityElement = self.getElementFromElement(Constant.XBRL_ENTITY, item)
            if(self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement, False) is None):
                periodElement = self.getElementFromElement(Constant.XBRL_PERIOD, item)
                instant = self.getValueAsDate(Constant.XBRL_INSTANT, periodElement) 
                if(instant is not None):
                    period =  PeriodEngine().getOrCreatePeriod3(instant, session)
                    periodDict[item['@id']] = period
        return periodDict

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