'''
Created on 19 sep. 2018

@author: afunes
'''
from nt import listdir

import pandas
import xmltodict

from dao.dao import Dao
from dao.entityFactDao import EntityFactDao
from engine.conceptEngine import ConceptEngine
from engine.periodEngine import PeriodEngine
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from modelClass.entityFactValue import EntityFactValue
from tools.tools import  getXSDFileFromCache
from valueobject.constant import Constant


class ImporterEntityFact(AbstractImporter, AbstractFactImporter):
    
    EXPLICIT_MEMBER_ALLOWED = ['CommonClassAMember', 'CommonStockMember', 'CapitalUnitClassAMember']

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_ENTITY_FACT, filename, replace, 'fileStatus', 'entityStatus')
        self.processCache = None
        self.conceptName = 'dei:EntityCommonStockSharesOutstanding'
            
    def doImport2(self):
        efvList = []
        self.processCache = self.initProcessCache3(self.filename, self.session)
        conceptElementList = self.getListFromElement([self.conceptName], self.processCache[Constant.DOCUMENT_INS], False)
        for conceptElement in conceptElementList:    
            conceptValue = self.getValueFromElement(['#text'], conceptElement, False)
            contextRef = self.getValueFromElement(['@contextRef'], conceptElement, False)
            entity = self.getElementFromElement([contextRef], self.processCache[Constant.ENTITY_DICT], False)
            segment = self.getElementFromElement(Constant.XBRL_SEGMENT, entity, False)
            explicitMemberValue = self.getValueFromElement(['#text'], self.getElementFromElement(Constant.XBRL_EXPLICIT_MEMBER, segment, False), False)
            
            concept = ConceptEngine().getOrCreateConcept("EntityCommonStockSharesOutstanding", self.session)
            entityFact = EntityFactDao().getEntityFact(concept.OID, 2803, 99, self.session) 
            
            efv = EntityFactValue()
            efv.entityFact = entityFact
            efv.fileData = self.fileData
            efv.value = conceptValue
            efv.period = self.processCache[Constant.PERIOD_DICT][contextRef]
            efv.explicitMember = explicitMemberValue
            efvList.append(efv)
        
        Dao().addObjectList(objectList = efvList, session = self.session)
        
        #reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
        #factVOList = self.getFactByConcept(reportDict, self.processCache, self.conceptName)
        #factVOList = self.setFactValues(factVOList, self.processCache)
        #EntityFactDao().addEntityFact(factVOList, self.fileData.OID , reportDict, self.session, self.replace)
        #return factVOList
        return efvList
            
    def addOrModifyFDError1(self, e):
        self.fileDataDao.addOrModifyFileData(entityStatus=e.status, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey, externalSession=self.session)
    
    def addOrModifyFDError2(self, e):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_ERROR, filename=self.filename, errorMessage=str(e)[0:149], errorKey=self.errorKey, externalSession=self.session)         
            
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(entityStatus=Constant.STATUS_INIT, filename=self.filename, errorKey=self.errorKey, externalSession=self.session)   
            
    def getPersistentList(self, voList):
        return []       
    
    def deleteImportedObject(self):
        EntityFactDao().deleteEFVByFD(self.fileData.OID, self.session)
            
    def getPeriodDict(self, xmlDictRoot, session):
        periodDict = {}
        for item in self.getListFromElement(Constant.XBRL_CONTEXT, xmlDictRoot):
            entityElement = self.getElementFromElement(Constant.XBRL_ENTITY, item)
            #self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement)
            segmentElement = self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement, False)
            explicitMemberElement = self.getObjectFromElement(Constant.XBRL_EXPLICIT_MEMBER, segmentElement)
            #if(segmentElement is None or self.isExplicitMemberAllowed(explicitMemberElement)):
            periodElement = self.getElementFromElement(Constant.XBRL_PERIOD, item)
            instant = self.getValueAsDate(Constant.XBRL_INSTANT, periodElement) 
            if(instant is not None):
                period =  PeriodEngine().getOrCreatePeriod3(instant, session)
                periodDict[item['@id']] = period
        return periodDict

    def isReportAllowed(self, reportRole):
#         keyList = ["DocumentAndEntityInformation", "CoverPage"]
#         for key in keyList:
#             if key.upper() in reportRole.upper(): 
#                 return True
#        return False
        return True


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
        
    def getObjectFromList(self, objectIDList, list_):
        for objectID in objectIDList:
            result = []
            for element in list_:
                if(element.get(objectID, None) is not None):
                    result.append(element)
            return result
        
    def isExplicitMemberAllowed(self, element):
        if (isinstance(element, list)):
            for e in element:
                value = self.getValueFromElement(['#text'], e, False)
                for em in self.EXPLICIT_MEMBER_ALLOWED:
                    if(value.find(em) != -1):
                        return True
        elif (isinstance(element, dict)):
            value = self.getValueFromElement(['#text'],element, False)
            for em in self.EXPLICIT_MEMBER_ALLOWED:
                if(value.find(em) != -1):
                    return True
            
        