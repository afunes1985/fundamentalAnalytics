'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_, or_
from sqlalchemy.sql.expression import text

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period
from valueobject.constant import Constant


class FactDao():
    
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
    def getFactValues2(reportShortName = None, CIK = None, ticker = None, conceptName = None, periodType = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'ticker' : ticker,
                       'reportShortName' : reportShortName,
                       'periodType' : periodType}
            
            query = text("""select * from(
                            select report.shortName as reportShortName, concept.conceptName, concept.label, factValue.value, 
                                    IFNULL(period.endDate, period.instant) date_, period.type as periodType, fact.order_ as order_
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
                                    and (:periodType is null or period.type = :periodType )
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
                                    and (:periodType is null or period.type = :periodType )) as rs
                                        order by reportShortName, conceptName, periodType, date_""")
            rs = con.execute(query, params)
            return rs 
        
    @staticmethod   
    def addFact(factVOList, company, fileData, reportDict, session, replace):
        objectAlreadyAdded = {}
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                #time1 = datetime.now()
                concept = Dao.getConcept(factVO.conceptName, session = session)
                if(concept == None):
                    concept = Concept()
                    concept.conceptName = factVO.conceptName
                    Dao.addObject(objectToAdd = concept, session = session, doFlush = True)
                factKey = str(company.OID) + "-" + str(concept.OID) + "-" + str(reportDict[factVO.reportRole].OID) + "-" + str(fileData.OID)
                if(objectAlreadyAdded.get(factKey, None) is None):
                    fact = FactDao.getFact(company, concept, reportDict[factVO.reportRole], fileData, session)
                    #fact = None
                    if(fact is None):
                        fact = Fact()
                        fact.companyOID = company.OID
                        fact.conceptOID = concept.OID
                        fact.reportOID = reportDict[factVO.reportRole].OID
                        fact.fileDataOID = fileData.OID
                        fact.order_ = factVO.order
#                     if(replace):
#                         for itemToDelete in fact.factValueList:
#                             session.delete(itemToDelete)
                    Dao.addObject(objectToAdd = fact, session = session, doFlush = True)
                    
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
    def getFact(company, concept, report, fileData, session):
        try:
            return GenericDao.getOneResult(Fact, and_(Fact.company == company, Fact.concept == concept, Fact.report == report, Fact.fileData == fileData), session)
        except NoResultFound:
            return None
    
    @staticmethod
    def getFactValue2(ticker, periodType = None, documentType = None, concept = None, session = None):
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
                             Concept.conceptName.__eq__(concept.conceptName)))\
                .order_by(Period.endDate)\
                .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus)\
                .distinct()\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
    