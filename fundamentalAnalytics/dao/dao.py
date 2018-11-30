'''
Created on 20 ago. 2018

@author: afunes
'''
from datetime import datetime
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import text, and_

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.abstractFactRelation import AbstractFactRelation
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.report import Report
from valueobject.constant import Constant


class GenericDao():
    @staticmethod
    def getFirstResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
    
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .first()
        return objectResult
    
    @staticmethod
    def getOneResult(objectClazz, condition = "", session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .one()
        return objectResult
    
    @staticmethod
    def getAllResult(objectClazz, condition = (1 == 1), session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .all()#.limit(100)\
        return objectResult

class DaoCompanyResult():
    
    @staticmethod
    def getFactValues(CIK = None, ticker = None, conceptName = None, periodType = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'ticker' : ticker,
                       'periodType' : periodType}
            query = text("""select distinct concept.conceptName, factValue.value, IFNULL(period.endDate, period.instant) date_
                                    FROM fa_fact fact
                                    left join fa_company company on fact.companyOID = company.OID
                                    left join fa_concept concept on fact.conceptOID = concept.OID
                                    left join fa_file_data fileData on fact.fileDataOID = fileData.OID
                                    left join fa_fact_value factValue on factValue.factOID = fact.OID
                                    left join fa_period period on factValue.periodOID = period.OID
                                where concept.conceptName = :conceptName
                                    and (company.CIK = :CIK or :CIK is null)
                                    and (company.ticker = :ticker or :ticker is null)
                                    and (period.type= :periodType or :periodType is null)
                                order by IFNULL(period.endDate, period.instant)""")
            rs = con.execute(query, params)
            return rs 
        
    @staticmethod
    def getFactValues2(CIK = None, ticker = None, conceptName = None, periodType = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'ticker' : ticker,
                       'periodType' : periodType}
#             query = text("""select report.shortName, concept.conceptName, concept.label, factValue.value, IFNULL(period.endDate, period.instant), period.type
#                                 FROM fa_fact fact
#                                     join fa_company company on fact.companyOID = company.OID
#                                     join fa_concept concept on fact.conceptOID = concept.OID
#                                     join fa_report report on fact.reportOID = report.OID
#                                     join fa_fact_value factValue on factValue.factOID = fact.OID
#                                     join fa_period period on factValue.periodOID = period.OID
#                                 where (concept.conceptName = :conceptName or :conceptName is null) 
#                                     and (company.CIK = :CIK or :CIK is null)
#                                     and (company.ticker = :ticker or :ticker is null)
#                                     and (period.type= :periodType or :periodType is null or period.type is null)
#                                 order by concept.conceptName, IFNULL(period.endDate, period.instant),  report.shortName,  fact.order""")
            query = text("""
                        CREATE TEMPORARY TABLE fact_report 
                        select report.shortName, concept.conceptName, concept.label, factValue.value, IFNULL(period.endDate, period.instant) date_, period.type
                            FROM fa_fact fact
                                join fa_company company on fact.companyOID = company.OID
                                join fa_concept concept on fact.conceptOID = concept.OID
                                join fa_report report on fact.reportOID = report.OID
                                join fa_fact_value factValue on factValue.factOID = fact.OID
                                join fa_period period on factValue.periodOID = period.OID
                            where 
                                 (company.ticker = "MSFT" )
                                    and (period.type is null or period.type = 'QTD');
                        
                        CREATE TEMPORARY TABLE fact_report2 select * from  fact_report;
                                                        
                        insert into fact_report
                        select temp.shortName, temp.conceptName, temp.label, temp.value, temp.date_, temp.type 
                            from(
                            select report.shortName, concept.conceptName, concept.label, factValue.value, IFNULL(period.endDate, period.instant) date_, period.type
                                FROM fa_fact fact
                                    join fa_company company on fact.companyOID = company.OID
                                    join fa_concept concept on fact.conceptOID = concept.OID
                                    join fa_report report on fact.reportOID = report.OID
                                    join fa_fact_value factValue on factValue.factOID = fact.OID
                                    join fa_period period on factValue.periodOID = period.OID
                                    left join fact_report2 freport on freport.conceptName = concept.conceptName and freport.date_ = IFNULL(period.endDate, period.instant)
                                where 
                                     company.ticker = "MSFT" 
                                        and period.type = 'YTD'
                                        and freport.conceptName is null) as temp;                    
                        
                        select * from fact_report
                        order by conceptName, date_, shortName;
                """)
            rs = con.execute(query, params)
            return rs 
    
class Dao():
    @staticmethod
    def getCompanyList(session = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            query = text("""select distinct company.CIK, company.entityRegistrantName, company.ticker
                                FROM fa_company company
                                    join fa_fact fact on fact.companyOID = company.OID
                                order by company.entityRegistrantName""")
            rs = con.execute(query, [])
            return rs 
    
    
    @staticmethod
    def getFactValue(fact, period, session):
        try:
            return GenericDao.getOneResult(FactValue, and_(FactValue.fact.__eq__(fact), FactValue.period.__eq__(period)), session)
        except NoResultFound:
            return FactValue()

    @staticmethod
    def getConceptOID(conceptName, session = None):
        try:
            return GenericDao.getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session).OID
        except NoResultFound:
            concept = Concept()
            concept.conceptName = conceptName
            Dao.addObject(objectToAdd = concept, session = session, doFlush = True)
            return concept.OID
    
    @staticmethod
    def getFact(company, concept, report, fileData, session):
        try:
            return GenericDao.getOneResult(Fact, and_(Fact.company == company, Fact.concept == concept, Fact.report == report, Fact.fileData == fileData), session)
        except NoResultFound:
            return None
        
    @staticmethod  
    def getReport(reportShortName, session):
        try:
            return GenericDao.getOneResult(Report, and_(Report.shortName == reportShortName), session)
        except NoResultFound:
            return None
    
    @staticmethod   
    def getFileData(filename, session = None):
        try:
            return GenericDao.getOneResult(FileData, and_(FileData.fileName == filename), session)
        except NoResultFound:
            return None
        
    @staticmethod   
    def addObject(objectToAdd, session = None, doCommit = False, doFlush = False):
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
    def addFact(factVOList, company, fileData, reportDict, session):
        objectAlreadyAdded = {}
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                #time1 = datetime.now()
                conceptOID = Dao.getConceptOID(factVO.conceptName, session = session)
                factKey = str(company.OID) + "-" + str(conceptOID) + "-" + str(reportDict[factVO.reportRole].OID) + "-" + str(fileData.OID)
                if(objectAlreadyAdded.get(factKey, None) is None):
                    #fact = Dao.getFact(company, concept, factVO.report, fileData, session)
                    #fact = None
                    #if(fact is None):
                    fact = Fact()
                    fact.companyOID = company.OID
                    fact.conceptOID = conceptOID
                    fact.reportOID = reportDict[factVO.reportRole].OID
                    fact.fileDataOID = fileData.OID
                    fact.order = factVO.order
                    session.add(fact) 
                    session.flush()
                    for factValueVO in factVO.factValueList:
                        #factValue = Dao.getFactValue(fact, factValueVO.period, session)
                        factValuekey =  str(factValueVO.period.OID) + "-" + str(fact.OID)
                        if(objectAlreadyAdded.get(factValuekey, None) is None):
                            factValue = FactValue()
                            factValue.value = factValueVO.value
                            factValue.period = factValueVO.period
                            #factValue.fact = fact
                            fact.factValueList.append(factValue)
                            objectAlreadyAdded[factValuekey] = ""
                    objectAlreadyAdded[factKey] = "" 
                    logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added fact" + str(factVO.conceptName))
                    #print("STEP 3.1 " + str(datetime.now() - time1))
                    #elif(len(factVO.factValueList) == 0):
                        #logging.getLogger(Constant.LOGGER_NONEFACTVALUE).debug("NoneFactValue " + fact.concept.conceptName + " " +  fileData.fileName)
        #session.commit()
        
        
    @staticmethod     
    def addAbstractConcept(factVO, session):
        try:
            abstractConcept =  GenericDao.getOneResult(AbstractConcept, and_(AbstractConcept.conceptName == factVO.conceptName), session)
        except NoResultFound:
            abstractConcept = AbstractConcept()
            abstractConcept.conceptName = factVO.conceptName
            Dao.addObject(objectToAdd = abstractConcept, session = session, doCommit = False)
        factVO.abstractConcept = abstractConcept
        return factVO
    
