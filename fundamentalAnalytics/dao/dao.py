'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import  and_, text

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept
from modelClass.customReport import CustomReport
from modelClass.factValue import FactValue
from modelClass.report import Report
from valueobject.constant import Constant


class GenericDao():
    
    def getFirstResult(self, objectClazz, condition, session=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .first()
        return objectResult
    
    def getOneResult(self, objectClazz, condition=(1 == 1), session=None, raiseNoResultFound=True):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        try:
            objectResult = session.query(objectClazz)\
            .filter(condition)\
            .one()
        except NoResultFound as e:
            if(raiseNoResultFound):
                raise e
            return None
        return objectResult
    
    def getAllResult(self, objectClazz, condition=(1 == 1), session=None, limit=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .limit(limit)\
        .all()
        return objectResult


class Dao():
    
    def addObjectList(self, objectList, session):
        if(len(objectList) > 0):
            for obj in objectList:
                Dao().addObject(objectToAdd=obj, session=session) 
            session.commit()
    
    def getConcept(self, conceptName, session=None):
        return GenericDao().getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session, raiseNoResultFound=False)
    
    def getReport(self, reportShortName, session):
        return GenericDao().getOneResult(Report, and_(Report.shortName == reportShortName), session, raiseNoResultFound=False)
    
    def addObject(self, objectToAdd, session=None, doCommit=False, doFlush=False):
        if(session is None):
            internalSession = DBConnector().getNewSession()
        else:
            internalSession = session
        internalSession.add(objectToAdd)
        if(doCommit):
            internalSession.commit()
        elif(doFlush):
            internalSession.flush()
        if(session is None):
            internalSession.close()
        logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added to DB " + str(objectToAdd))

    @staticmethod     
    def addAbstractConcept(factVO, session):
        try:
            abstractConcept = GenericDao().getOneResult(AbstractConcept, and_(AbstractConcept.conceptName == factVO.conceptName), session)
        except NoResultFound:
            abstractConcept = AbstractConcept()
            abstractConcept.conceptName = factVO.conceptName
            Dao().addObject(objectToAdd=abstractConcept, session=session, doCommit=False)
        factVO.abstractConcept = abstractConcept
        return factVO
    
    def getCustomConcept(self, customConceptName, session=None):
        return GenericDao().getOneResult(CustomConcept, CustomConcept.conceptName.__eq__(customConceptName), session, raiseNoResultFound=False)

    def getCustomReport(self, reportShortName, session=None):
        return GenericDao().getOneResult(CustomReport, and_(CustomReport.shortName == reportShortName), session, raiseNoResultFound=False)
    
    def getValuesForExpression(self, fileDataOID, session):
        params = { 'fileDataOID' : fileDataOID}
        query = text(""" select concept.conceptName,  fv.value, period.OID as periodOID
                        FROM fa_custom_fact fact
                            join fa_custom_fact_value fv on fv.customFactOID = fact.OID
                            join fa_custom_concept concept on fact.customConceptOID = concept.OID
                            join fa_period period on fv.periodOID = period.OID
                        where fv.fileDataOID = :fileDataOID
                        union
                        select concept.conceptName,  efv.value, null as periodOID
                        FROM fa_entity_fact ef
                            join fa_entity_fact_value efv on ef.oid = efv.entityFactOID
                            join fa_concept concept on ef.conceptOID = concept.OID
                            join fa_explicit_member em on em.oid = efv.explicitMemberOID
                        where efv.fileDataOID = :fileDataOID
                            and em.explicitMemberValue = 'NO_EXPLICIT_MEMBER'
                        union
                        select 'PRICE', p.value, null as periodOID
                        from fa_price p
                        where p.fileDataOID = :fileDataOID""")
        return session.execute(query, params)
    
    def getValuesForApp4(self, CIK, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        params = { 'CIK' : CIK}
        query = text(""" select conceptName,  value, endDate, explicitMemberValue
                        from (select concept.conceptName,  efv.value, p.instant as endDate, em.explicitMemberValue
                        FROM fa_entity_fact ef
                            join fa_entity_fact_value efv on ef.oid = efv.entityFactOID
                            join fa_concept concept on ef.conceptOID = concept.OID
                            join fa_period p on p.oid = efv.periodOID
                            join fa_file_Data fd on fd.oid = efv.fileDataOID
                            join fa_company c on c.oid = fd.companyOID
                            join fa_ticker t on t.companyOID = c.OID
                            join fa_explicit_member em on em.oid = efv.explicitMemberOID
                        where c.CIK = :CIK
                        union
                        select 'PRICE', pri.value, p.instant as endDate, null as explicitMemberValue
                        from fa_price pri
                            join fa_period p on p.oid = pri.periodOID
                            join fa_file_Data fd on fd.oid = pri.fileDataOID
                            join fa_company c on c.oid = fd.companyOID
                            join fa_ticker t on t.companyOID = c.OID
                        where c.CIK = :CIK) as a
                        order by conceptName, explicitMemberValue""")
        return session.execute(query, params)
