'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import text, and_

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.abstractFactRelation import AbstractFactRelation
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
    def getOneResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .one()
        return objectResult
    
    @staticmethod
    def getAllResult(objectClazz, condition, session = None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(objectClazz)\
        .filter(condition)\
        .all()
        return objectResult

class DaoCompanyResult():
    
    @staticmethod
    def getFactValues(CIK = None, ticker = None, conceptName = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'ticker' : ticker}
            query = text("""select distinct concept.conceptName, factValue.value, IFNULL(period.endDate, period.instant) date_
                                    FROM fa_fact fact
                                    left join fa_company company on fact.companyOID = company.OID
                                    left join fa_concept concept on fact.conceptOID = concept.OID
                                    left join fa_file_data fileData on fact.fileDataOID = fileData.OID
                                    left join fa_fact_value factValue on factValue.factOID = fact.OID
                                    left join fa_period period on factValue.periodOID = period.OID
                                where company.ticker = 'TSLA'
                                    and concept.conceptName = :conceptName
                                    and (company.CIK = :CIK or :CIK is null)
                                    and (company.ticker = :ticker or :ticker is null)
                                order by IFNULL(period.endDate, period.instant)""")
            rs = con.execute(query, params)
            return rs 
    
class Dao():
    @staticmethod
    def getFactValue(fact, period, session):
        try:
            return GenericDao.getOneResult(FactValue, and_(FactValue.fact.__eq__(fact), FactValue.period.__eq__(period)), session)
        except NoResultFound:
            return FactValue()

    @staticmethod
    def getConcept(conceptName, session):
        try:
            return GenericDao.getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session)
        except NoResultFound:
            concept = Concept()
            concept.conceptName = conceptName
            session.add(concept)
            session.flush()
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added concept" + conceptName)
            return concept
    
    @staticmethod
    def getFact(company, concept, report, fileData, session):
        try:
            return GenericDao.getOneResult(Fact, and_(Fact.company == company, Fact.concept == concept, Fact.report == report, Fact.fileData == fileData), session)
        except NoResultFound:
            return Fact()
        
    @staticmethod  
    def getReport(reportShortName, session):
        try:
            return GenericDao.getOneResult(Report, and_(Report.shortName == reportShortName), session)
        except NoResultFound:
            report = Report()
            report.shortName = reportShortName
            session.add(report)
            session.flush()
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added report " + reportShortName)
            return report
    
    @staticmethod   
    def getFileData(filename, session = None):
        try:
            return GenericDao.getOneResult(FileData, and_(FileData.fileName == filename), session)
        except NoResultFound:
            return None
        
    @staticmethod   
    def addObject(objectToAdd, session = None, doCommit = False):
        if(session is None):
            session = DBConnector().getNewSession()
        session.add(objectToAdd)
        if(doCommit):
            session.commit()
        else:
            session.flush()
        logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added to DB " + str(objectToAdd))

        
    @staticmethod   
    def addFact(factVOList, company, fileData, session):
        for factVO in factVOList:
            concept = Dao.getConcept(factVO.conceptName, session)
            fact = Dao.getFact(company, concept, factVO.report, fileData, session)
            fact.company = company
            fact.concept = concept
            fact.report = factVO.report
            fact.order = factVO.order
            fact.fileData = fileData
            if (len(factVO.factValueList) > 0): 
                for factValueVO in factVO.factValueList:
                    factValue = Dao.getFactValue(fact, factValueVO.period, session)
                    factValue.value = factValueVO.value
                    factValue.period = factValueVO.period
                    factValue.fact = fact
                    fact.factValueList.append(factValue)
            elif(len(factVO.factValueList) == 0):
                logging.getLogger(Constant.LOGGER_NONEFACTVALUE).debug("NoneFactValue " + fact.concept.conceptName + " " +  fileData.fileName)
            session.add(fact)
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added fact" + str(factVO.conceptName))
        session.commit()
        
    @staticmethod     
    def addAbstractConcept(factVO, session):
        try:
            abstractConcept =  GenericDao.getOneResult(AbstractConcept, and_(AbstractConcept.conceptName == factVO.conceptName), session)
        except NoResultFound:
            abstractConcept = AbstractConcept()
            abstractConcept.conceptName = factVO.conceptName
            session.add(abstractConcept)
            session.flush()
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added abstractConcept " + abstractConcept.conceptName)
        factVO.abstractConcept = abstractConcept
        return factVO
    
    @staticmethod
    def addAbstractFactRelation(factVO, session):
        try:
            abstractFactRelation =  GenericDao.getOneResult(AbstractFactRelation, and_(AbstractFactRelation.OID == 99), session)
        except NoResultFound:
            abstractFactRelation = AbstractFactRelation()
            abstractFactRelation.abstractFromOID = factVO.abstractFromOID
            session.add(abstractFactRelation)
            session.flush()
            logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added abstractFactRelation " + abstractFactRelation.conceptName)
        factVO.abstractConcept = abstractFactRelation
        return factVO

