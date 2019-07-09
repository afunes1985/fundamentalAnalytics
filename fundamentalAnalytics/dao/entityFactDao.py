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
                    entityFact = self.getEntityFact(concept.OID, fileDataOID, session)
                    if(entityFact is None):
                        entityFact = EntityFact()
                        entityFact.companyOID = company.OID
                        entityFact.conceptOID = concept.OID
                        entityFact.report = reportDict[factVO.reportRole]
                        entityFact.fileDataOID = fileDataOID
                        entityFact.order_ = factVO.order
                        Dao().addObject(objectToAdd = entityFact, session = session, doFlush = True)
                    else:
                        for factValue in entityFact.entityFactValueList:
                            factValuekey =  str(factValue.periodOID) + "-" + str(entityFact.OID)
                            objectAlreadyAdded[factValuekey] = ""
                    
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
        session.commit()
        
    def getEntityFact(self, conceptOID, fileDataOID, session):
        return GenericDao.getOneResult(EntityFact, and_(EntityFact.conceptOID == conceptOID, EntityFact.fileDataOID == fileDataOID), session, raiseNoResultFound = False)
        
    def getEntityFactList(self, ticker, conceptName, priceStatus, session):
        try:
            if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
            query = session.query(EntityFactValue)\
                .join(EntityFactValue.entityFact)\
                .join(EntityFact.concept)\
                .join(EntityFact.fileData)\
                .join(EntityFactValue.period)\
                .join(FileData.company)\
                .filter(and_(Concept.conceptName.__eq__(conceptName), FileData.priceStatus == priceStatus, or_(ticker == "", Company.ticker == ticker), Company.ticker.isnot(None)))\
                .order_by(Period.endDate)\
                .with_entities(Company.ticker, FileData.fileName, EntityFactValue.periodOID, Period.instant, EntityFact.fileDataOID)\
                .distinct()
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
    