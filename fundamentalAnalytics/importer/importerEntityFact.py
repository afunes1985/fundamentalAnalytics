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
        self.fileDataDao.addOrModifyFileData(entityStatus=e.status, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey)
    
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_ERROR, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey)         
            
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_INIT, priceStatus=Constant.STATUS_PENDING, filename=self.filename, errorKey=self.errorKey)   
            
    def getPersistentList(self, voList):
        return []       
            
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
        
                
    def getFactByReport(self, reportDict, processCache, session):
        factVOList = []
        # Obtengo para cada reporte sus conceptos
        xmlDictPre = processCache[Constant.DOCUMENT_PRE]
        for item in self.getListFromElement(Constant.PRESENTATON_LINK, self.getElementFromElement(Constant.LINKBASE, xmlDictPre)): 
            tempFactVOList = []
            reportRole = item['@xlink:role']
            isReportAllowed = False 
            if(reportDict.get(reportRole, None) is not None):
                presentationDF = pandas.DataFrame(self.getListFromElement(Constant.PRESENTATIONARC, item))
                presentationDF.set_index("@xlink:to", inplace=True)
                presentationDF.head()
                for item2 in self.getListFromElement(Constant.LOC, item):
                    factVO = FactVO()
                    factVO.xlink_href = item2["@xlink:href"]
                    factVO.reportRole = reportRole
                    factVO.labelID = item2["@xlink:label"]
                    factVO = self.setXsdValue(factVO, processCache)
                    if factVO.abstract != "true":
                        try:
                            factVO.order = self.getValueFromElement(["@order"], presentationDF.loc[factVO.labelID], True) 
                            tempFactVOList.append(factVO)
                        except Exception as e:
                            logging.getLogger(Constant.LOGGER_ERROR).debug("Error " + str(e))
                    if(self.isReportAllowed2(factVO.xlink_href)):
                        isReportAllowed = True
                
            if(not isReportAllowed):
                try:
                    del reportDict[reportRole]
                except KeyError as e:
                    pass
            else:
                factVOList = factVOList + tempFactVOList
        for report in reportDict.values():
            Dao().addObject(objectToAdd=report, session=session, doFlush=True)
        return factVOList
        
    def initProcessCache(self, filename, session):
        processCache = {}
        processCache.update(self.cacheDict.get("XSD_CACHE", None))
        schDF = pandas.DataFrame(self.getListFromElement(Constant.ELEMENT, self.getElementFromElement(Constant.SCHEMA, getXMLDictFromGZCache(filename, Constant.DOCUMENT_SCH))))
        schDF.set_index("@id", inplace=True)
        schDF.head()
        processCache[Constant.DOCUMENT_SCH] = schDF
        # XML INSTANCE
        insDict = getXMLDictFromGZCache(filename, Constant.DOCUMENT_INS)
        insDict = self.getElementFromElement(Constant.XBRL_ROOT, insDict)
        processCache[Constant.DOCUMENT_INS] = insDict
        # XML SUMMARY
        sumDict = getXMLDictFromGZCache(filename, Constant.DOCUMENT_SUMMARY)
        processCache[Constant.DOCUMENT_SUMMARY] = sumDict
        # XML PRESENTATION
        preDict = getXMLDictFromGZCache(filename, Constant.DOCUMENT_PRE)
        processCache[Constant.DOCUMENT_PRE] = preDict 
        # PERIOD
        periodDict = self.getPeriodDict(insDict, session)
        processCache[Constant.PERIOD_DICT] = periodDict 
        # COMPANY
        CIK = self.getValueFromElement(['#text'], self.getElementFromElement(['dei:EntityCentralIndexKey'], insDict, False), False)
        entityRegistrantName = self.getValueFromElement(['#text'], self.getElementFromElement(['dei:EntityRegistrantName'], insDict, False), False) 
        ticker = self.getValueFromElement(['#text'], self.getElementFromElement(['dei:TradingSymbol'], insDict, False), False)
        processCache["COMPANY"] = CompanyEngine().getOrCreateCompany(CIK, ticker, entityRegistrantName, session)
        return processCache
    
    def setFactValues(self, factToAddList, processCache):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        periodDict = processCache[Constant.PERIOD_DICT]
        logging.getLogger(Constant.LOGGER_GENERAL).debug("periodDict " + str(periodDict))
        objectToDelete = []
        for factVO in factToAddList:
            conceptID = factVO.xlink_href[factVO.xlink_href.find("#", 0) + 1:len(factVO.xlink_href)]
            try:
                element = insXMLDict[conceptID.replace("_", ":")]
                if isinstance(element, list):
                    for element1 in element:
                        factValue = self.getFactValue(periodDict, element1)
                        if (factValue is not None):
                            factVO.factValueList.append(factValue)
                else:
                    factValue = self.getFactValue(periodDict, element)
                    if (factValue is not None):
                        factVO.factValueList.append(factValue)
                if(len(factVO.factValueList) == 0):
                    objectToDelete.append(factVO)
            except KeyError as e:
                pass
                # logging.getLogger(Constant.LOGGER_ERROR).debug("KeyError " + str(e) + " " + conceptID )
        factToAddList = [x for x in factToAddList if x not in objectToDelete]
        return factToAddList
    
    def getFactByConcept(self, reportDict, processCache, conceptName):
        factVOList = []
        # Obtengo para cada reporte sus conceptos
        xmlDictPre = processCache[Constant.DOCUMENT_PRE]
        for item in self.getListFromElement(Constant.PRESENTATON_LINK, self.getElementFromElement(Constant.LINKBASE, xmlDictPre)): 
            reportRole = item['@xlink:role']
            if(reportDict.get(reportRole, None) is not None):
                for item2 in self.getListFromElement(Constant.LOC, item):
                    href = item2["@xlink:href"]
                    if(href.find(conceptName) != -1):
                        factVO = FactVO()
                        factVO.xlink_href = href
                        factVO.reportRole = reportRole
                        factVO.labelID = item2["@xlink:label"]
                        factVO.order = 99
                        factVO = self.setXsdValue(factVO, processCache)
                        factVOList.append(factVO)
                        return factVOList
        return factVOList
    
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
