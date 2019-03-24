'''
Created on 20 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import or_
from sqlalchemy.sql.expression import text, and_
from sqlalchemy.sql.operators import in_op

from base.dbConnector import DBConnector
from modelClass.abstractConcept import AbstractConcept
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.customConcept import CustomConcept
from modelClass.customFact import CustomFact
from modelClass.customFactValue import CustomFactValue
from modelClass.customReport import CustomReport
from modelClass.expression import Expression
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period
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
                                    and (period.type= :periodType or :periodType is null or period.type is null)
                                order by IFNULL(period.endDate, period.instant)""")
            rs = con.execute(query, params)
            return rs 
        
    @staticmethod
    def getFactValues2(reportShortName = None, CIK = None, ticker = None, conceptName = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'ticker' : ticker,
                       'reportShortName' : reportShortName}
            
            query = text("""select * from(
                            select report.shortName as reportShortName, concept.conceptName, concept.label, factValue.value, 
                                    IFNULL(period.endDate, period.instant) date_, period.type as periodType, null as order_
                                 FROM fa_fact fact
                                     join fa_company company on fact.companyOID = company.OID
                                     join fa_concept concept on fact.conceptOID = concept.OID
                                     join fa_report report on fact.reportOID = report.OID
                                     join fa_fact_value factValue on factValue.factOID = fact.OID
                                     join fa_period period on factValue.periodOID = period.OID
                                 where 
                                    (:conceptName is null or concept.conceptName = :conceptName)
                                    and (:ticker is null or company.ticker = :ticker )
                                    and (:reportShortName is null or report.shortName = :reportShortName )
                                    and (period.type = 'QTD' or period.type = 'YTD' or period.type is null)  
                            union
                            select report.shortName as reportShortName, concept.conceptName, concept.label, factValue.value, 
                                    IFNULL(period.endDate, period.instant) date_, period.type as periodType, concept.defaultOrder as order_
                                 FROM fa_custom_fact fact
                                     join fa_company company on fact.companyOID = company.OID
                                     join fa_custom_concept concept on fact.customConceptOID = concept.OID
                                     join fa_custom_report report on fact.customReportOID = report.OID
                                     join fa_custom_fact_value factValue on factValue.customFactOID = fact.OID
                                     join fa_period period on factValue.periodOID = period.OID
                                 where
                                    (:conceptName is null or concept.conceptName = :conceptName)
                                    and (:ticker is null or company.ticker = :ticker)
                                    and (:reportShortName is null or report.shortName = :reportShortName )
                                    and (period.type = 'QTD' or period.type = 'YTD')) as rs
                                        order by reportShortName, conceptName, periodType, order_, date_""")
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
    def getCompany(ticker, session):
        try:
            return GenericDao.getOneResult(Company, and_(Company.ticker.__eq__(ticker)), session)
        except NoResultFound:
            return None

    @staticmethod
    def getConcept(conceptName, session = None):
        try:
            return GenericDao.getOneResult(Concept, Concept.conceptName.__eq__(conceptName), session)
        except NoResultFound:
            
            return None
    
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
                concept = Dao.getConceptOID(factVO.conceptName, session = session)
                if(concept == None):
                    concept = Concept()
                    concept.conceptName = factVO.conceptName
                    Dao.addObject(objectToAdd = concept, session = session, doFlush = True)
                factKey = str(company.OID) + "-" + str(concept.OID) + "-" + str(reportDict[factVO.reportRole].OID) + "-" + str(fileData.OID)
                if(objectAlreadyAdded.get(factKey, None) is None):
                    #fact = Dao.getFact(company, concept, factVO.report, fileData, session)
                    #fact = None
                    #if(fact is None):
                    fact = Fact()
                    fact.companyOID = company.OID
                    fact.conceptOID = concept.OID
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
    
    @staticmethod 
    def getCustomFactValuesFromConcept(ticker = None, periodType = None, conceptNames = None):
        try:
            dbconnector = DBConnector()
            cursor = dbconnector.engine.connect().connection.cursor()
            query = "fa_getCustomValuesFromConcepts"
            params = [ticker, periodType, conceptNames, 2]
            cursor.callproc(query, params)
            rs = cursor.stored_results()
            for row in rs:
                return (row.fetchall())
        except NoResultFound:
            return None
    
    @staticmethod 
    def test():
        dbconnector = DBConnector()
        cursor = dbconnector.engine.connect().connection.cursor()
        args = (5, 6, 0) # 0 is to hold value of the OUT parameter sum
        cursor.callproc('add', args)
        
    @staticmethod
    def getCustomConcept(conceptName, session = None):
        try:
            return GenericDao.getOneResult(CustomConcept, CustomConcept.conceptName.__eq__(conceptName), session)
        except NoResultFound:
            return None

    @staticmethod  
    def getCustomReport(reportShortName, session = None):
        try:
            return GenericDao.getOneResult(CustomReport, and_(CustomReport.shortName == reportShortName), session)
        except NoResultFound:
            return None
    
    @staticmethod
    def getCustomFact(company, concept, report, session):
        try:
            return GenericDao.getOneResult(CustomFact, and_(CustomFact.company == company, CustomFact.customConcept == concept, CustomFact.customReport == report), session)
        except NoResultFound:
            return None
    
    @staticmethod
    def getExpression(expressionName, session = None):
        try:
            return GenericDao.getOneResult(Expression, Expression.name == expressionName, session)
        except NoResultFound:
            return None
        
    @staticmethod
    def getCustomFactValue(ticker, customConceptName, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(CustomFactValue)\
                .join(CustomFactValue.customFact)\
                .join(CustomFact.customConcept)\
                .join(CustomFact.company)\
                .filter(and_(Company.ticker.__eq__(ticker), CustomConcept.conceptName.__eq__(customConceptName), Period.type.__eq__(periodType)))\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
    
    @staticmethod
    def getFactValue2(ticker, periodType = None, documentType = None, conceptList = None, session = None):
        list = (c.conceptName for c in conceptList)
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(FactValue)\
                .join(FactValue.fact)\
                .join(Fact.concept)\
                .join(Fact.company)\
                .join(Fact.report)\
                .join(FactValue.period)\
                .join(Fact.fileData)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType), \
                             or_(FileData.documentType.__eq__(documentType), documentType == None), \
                             in_op(Concept.conceptName, list)))\
                .order_by(Period.endDate)\
                .with_entities(FactValue.value, FactValue.periodOID)\
                .distinct()\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
    
    @staticmethod    
    def getPeriodByFact(ticker, conceptName, periodType = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(Period)\
                .join(Period.factValueList)\
                .join(FactValue.fact)\
                .join(Fact.concept)\
                .join(Fact.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType)))\
                .order_by(Period.endDate)\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()