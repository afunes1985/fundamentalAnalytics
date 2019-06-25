'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_, or_

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.entityFact import EntityFact
from modelClass.entityFactValue import EntityFactValue
from modelClass.fileData import FileData
from modelClass.period import Period
from valueobject.constant import Constant


class EntityFactDao():
    
    def addEntityFact(self, factVOList, company, fileDataOID, reportDict, session, replace):
        objectAlreadyAdded = {}
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                concept = Dao.getConcept(factVO.conceptName, session = session)
                if(concept == None):
                    concept = Concept()
                    concept.conceptName = factVO.conceptName
                    Dao().addObject(objectToAdd = concept, session = session, doFlush = True)
                factKey = str(company.OID) + "-" + str(concept.OID) + "-" + str(reportDict[factVO.reportRole].OID) + "-" + str(fileDataOID)
                if(objectAlreadyAdded.get(factKey, None) is None):
                    entityFact = self.getEntityFact(company, concept, reportDict[factVO.reportRole], fileDataOID, session)
                    if(entityFact is None):
                        entityFact = EntityFact()
                        entityFact.companyOID = company.OID
                        entityFact.conceptOID = concept.OID
                        entityFact.reportOID = reportDict[factVO.reportRole].OID
                        entityFact.fileDataOID = fileDataOID
                        entityFact.order_ = factVO.order
                    Dao().addObject(objectToAdd = entityFact, session = session, doFlush = True)
                    
                    for factValueVO in factVO.factValueList:
                        factValuekey =  str(factValueVO.period.OID) + "-" + str(entityFact.OID)
                        if(objectAlreadyAdded.get(factValuekey, None) is None):
                            entityFactValue = EntityFactValue()
                            entityFactValue.value = factValueVO.value
                            entityFactValue.period = factValueVO.period
                            entityFact.entityFactValueList.append(entityFactValue)
                            objectAlreadyAdded[factValuekey] = ""
                    objectAlreadyAdded[factKey] = "" 
                    logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added entityFact" + str(factVO.conceptName))
                    if(len(factVO.factValueList) == 0):
                        logging.getLogger(Constant.LOGGER_NONEFACTVALUE).debug("NoneFactValue " + entityFact.concept.conceptName)
        #session.commit()
        
    def getEntityFact(self, company, concept, report, fileDataOID, session):
        try:
            return GenericDao.getOneResult(EntityFact, and_(EntityFact.company == company, EntityFact.concept == concept, EntityFact.report == report, EntityFact.fileDataOID == fileDataOID), session)
        except NoResultFound:
            return None
    