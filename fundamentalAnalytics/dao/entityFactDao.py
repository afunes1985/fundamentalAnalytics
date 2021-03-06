'''
Created on Apr 19, 2019

@author: afunes
'''

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.functions import func

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from engine.conceptEngine import ConceptEngine
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.entityFact import EntityFact
from modelClass.entityFactValue import EntityFactValue
from modelClass.explicitMember import ExplicitMember
from modelClass.fileData import FileData
from modelClass.period import Period


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
        
    def getEntityFactValueList(self, fileDataOID, conceptOID, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        query = session.query(EntityFactValue)\
            .join(EntityFactValue.fileData)\
            .filter(and_(Concept.OID.__eq__(conceptOID), EntityFactValue.fileDataOID == fileDataOID))
        objectResult = query.all()
        return objectResult
        
    def deleteEFVByFD(self, fileDataOID, session=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        session.query(EntityFactValue).filter(and_(EntityFactValue.fileDataOID == fileDataOID)).delete()
    
    
    def getFirstEntityFact(self, fileDataOID, conceptName, session):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        try:
            query = session.query(EntityFactValue)\
                .join(EntityFactValue.explicitMember)\
                .filter(and_(Concept.conceptName.__eq__(conceptName), 
                             EntityFactValue.fileDataOID == fileDataOID))\
                .order_by(ExplicitMember.order_)
            objectResult = query.first()
        except NoResultFound:
            objectResult = None
        return objectResult
    
        
    def getEntityFactValueForReport(self, CIK, session=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        try:
            query = session.query(EntityFactValue)\
                .join(EntityFactValue.explicitMember)\
                .join(EntityFactValue.fileData)\
                .join(EntityFactValue.period)\
                .join(FileData.company)\
                .filter(and_(Company.CIK.__eq__(CIK)))\
                .order_by(func.ifnull(Period.endDate, Period.instant),ExplicitMember.order_)\
                .with_entities(func.date_format(func.ifnull(Period.endDate, Period.instant), '%Y-%m-%d').label("period"),func.format(EntityFactValue.value, 0).label("value"), ExplicitMember.explicitMemberValue, ExplicitMember.order_)
            objectResult = query.all()
        except NoResultFound:
            objectResult = None
        return objectResult