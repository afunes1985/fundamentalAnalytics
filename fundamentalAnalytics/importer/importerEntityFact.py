'''
Created on 19 sep. 2018

@author: afunes
'''
from nt import listdir

import pandas
import xmltodict

from dao.dao import Dao, GenericDao
from dao.entityFactDao import EntityFactDao
from engine.conceptEngine import ConceptEngine
from engine.periodEngine import PeriodEngine
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from modelClass.entityFactValue import EntityFactValue
from modelClass.explicitMember import ExplicitMember
from tools.tools import  getXSDFileFromCache
from valueobject.constant import Constant


class ImporterEntityFact(AbstractImporter, AbstractFactImporter):
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_ENTITY_FACT, filename, replace, 'companyStatus', 'entityStatus')
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
            explicitMemberElement = self.getObjectFromElement(Constant.XBRL_EXPLICIT_MEMBER, segment)
            if(explicitMemberElement is None or isinstance(explicitMemberElement, dict)):
                explicitMemberValue = self.getValueFromElement(['#text'], explicitMemberElement, False)
                concept = ConceptEngine().getOrCreateConcept("EntityCommonStockSharesOutstanding", self.session)
                entityFact = EntityFactDao().getEntityFact(concept.OID, 2803, 99, self.session) 
                efv = EntityFactValue()
                efv.entityFact = entityFact
                efv.fileData = self.fileData
                if conceptValue is None:
                    efv.value = 0
                else:
                    efv.value = conceptValue
                efv.period = self.processCache[Constant.PERIOD_DICT][contextRef]
                if(explicitMemberValue is None):
                    efv.explicitMember = GenericDao().getOneResult(ExplicitMember, (ExplicitMember.explicitMemberValue == 'NO_EXPLICIT_MEMBER'), self.session, raiseNoResultFound = True)
                else:
                    efv.explicitMember = GenericDao().getOneResult(ExplicitMember, (ExplicitMember.explicitMemberValue == explicitMemberValue), self.session, raiseNoResultFound = False)
                    if(efv.explicitMember is None):
                        em = ExplicitMember(explicitMemberValue=explicitMemberValue, order_=99)
                        Dao().addObject(objectToAdd=em, session=self.session)
                        efv.explicitMember = em
                efvList.append(efv)
        Dao().addObjectList(objectList = efvList, session = self.session)
        return efvList
            
    def getPersistentList(self, voList):
        return []       
    
    def deleteImportedObject(self):
        EntityFactDao().deleteEFVByFD(self.fileData.OID, self.session)
            
    def getPeriodDict(self, xmlDictRoot, session):
        try:
            periodDict = {}
            for item in self.getListFromElement(Constant.XBRL_CONTEXT, xmlDictRoot):
                #entityElement = self.getElementFromElement(Constant.XBRL_ENTITY, item)
                #self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement)
                #segmentElement = self.getElementFromElement(Constant.XBRL_SEGMENT, entityElement, False)
                #explicitMemberElement = self.getObjectFromElement(Constant.XBRL_EXPLICIT_MEMBER, segmentElement)
                #if(segmentElement is None or self.isExplicitMemberAllowed(explicitMemberElement)):
                periodElement = self.getElementFromElement(Constant.XBRL_PERIOD, item)
                instant = self.getValueAsDate(Constant.XBRL_INSTANT, periodElement) 
                if(instant is not None):
                    period =  PeriodEngine().getOrCreatePeriod3(instant, session)
                    periodDict[item['@id']] = period
        except Exception as e:
            print(e)
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
    
    def skipOrProcess(self):
        if (self.fileData.company is not None and self.fileData.company.listed):
            #self.previousStatus is None for fileStatus
            if(self.previousStatus is None or (getattr(self.fileData, self.previousStatus) in [Constant.STATUS_OK, Constant.STATUS_WARNING, Constant.STATUS_NO_DATA])):
                if (getattr(self.fileData, self.actualStatus) != Constant.STATUS_OK or self.replace == True):
                    return True
                else:
                    return False
            else:
                return False  
        else:
            return False
        