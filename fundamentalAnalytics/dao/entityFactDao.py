'''
Created on Apr 19, 2019

@author: afunes
'''

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
from engine.conceptEngine import ConceptEngine


class EntityFactDao():
    
    def addEntityFact(self, factVOList, fileDataOID, reportDict, session, replace):
        objectAlreadyAdded = {}
        entityFactValueList = []
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                concept = ConceptEngine().getOrCreateConcept(factVO.conceptName, session)
                efvList = EntityFactDao().getEntityFactValueList(fileDataOID, concept.OID, session)
                for efv in efvList:
                    factValueKey = str(efv.fileDataOID) + "-" + str(efv.entityFact.conceptOID)
                    objectAlreadyAdded[factValueKey] = ""
                entityFact = None
                if (reportDict[factVO.reportRole].OID is not None):
                    entityFact = self.getEntityFact(concept.OID, reportDict[factVO.reportRole].OID, factVO.order, session)
                if(entityFact is None):
                    entityFact = EntityFact()
                    entityFact.conceptOID = concept.OID
                    entityFact.report = reportDict[factVO.reportRole]
                    entityFact.order_ = factVO.order
                    Dao().addObject(objectToAdd=entityFact, session=session, doFlush=True)
                
                for factValueVO in factVO.factValueList:
                    factValueKey = str(fileDataOID) + "-" + str(concept.OID)
                    if(objectAlreadyAdded.get(factValueKey, None) is None):
                        entityFactValue = EntityFactValue()
                        entityFactValue.value = factValueVO.value
                        entityFactValue.period = factValueVO.period
                        entityFactValue.fileDataOID = fileDataOID
                        entityFactValue.entityFact = entityFact
                        entityFactValueList.append(entityFactValue)
                        objectAlreadyAdded[factValueKey] = ""
        for efv in entityFactValueList:
            Dao().addObject(objectToAdd=efv, session=session)            
        session.commit()
        
    def getEntityFact(self, conceptOID, reportOID, order_, session):
        return GenericDao().getOneResult(EntityFact, and_(EntityFact.conceptOID == conceptOID, EntityFact.reportOID == reportOID, EntityFact.order_ == order_), session, raiseNoResultFound=False)
        
    def getEntityFactList(self, ticker, conceptName, priceStatus, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(EntityFactValue)\
            .join(EntityFactValue.entityFact)\
            .join(EntityFact.concept)\
            .join(EntityFactValue.fileData)\
            .join(EntityFactValue.period)\
            .join(FileData.company)\
            .filter(and_(Concept.conceptName.__eq__(conceptName), FileData.priceStatus == priceStatus, or_(ticker == "", Company.ticker == ticker), Company.ticker.isnot(None)))\
            .order_by(Period.endDate)\
            .with_entities(Company.ticker, FileData.fileName, EntityFactValue.periodOID, Period.instant, EntityFactValue.fileDataOID)\
            .distinct()
        objectResult = query.all()
        return objectResult
        
    def getEntityFactValueList(self, fileDataOID, conceptOID, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(EntityFactValue)\
            .join(EntityFactValue.fileData)\
            .filter(and_(Concept.OID.__eq__(conceptOID), EntityFactValue.fileDataOID == fileDataOID))
        objectResult = query.all()
        return objectResult
        
    def getEntityFactList2(self, ticker, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(EntityFactValue)\
            .join(EntityFactValue.entityFact)\
            .join(EntityFact.concept)\
            .join(EntityFactValue.fileData)\
            .join(EntityFactValue.period)\
            .join(FileData.company)\
            .filter(or_(ticker == "", Company.ticker == ticker))
        objectResult = query.all()
        return objectResult
    
    def deleteEFVByFD(self, fileDataOID, session=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        session.query(EntityFactValue).filter(and_(EntityFactValue.fileDataOID == fileDataOID)).delete()
    
    
    def getEntityFact2(self, fileDataOID, conceptName, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(EntityFactValue)\
            .join(EntityFactValue.fileData)\
            .filter(and_(Concept.conceptName.__eq__(conceptName), EntityFactValue.fileDataOID == fileDataOID))
        objectResult = query.one()
        return objectResult