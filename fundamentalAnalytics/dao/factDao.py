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
from engine.conceptEngine import ConceptEngine
from modelClass.company import Company
from modelClass.concept import Concept
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period
from valueobject.constant import Constant


class FactDao():
    
    @staticmethod
    def getFactValues(CIK=None, ticker=None, conceptName=None, periodType=None):
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
    def getFactValues2(reportShortName=None, CIK=None, ticker=None, conceptName=None, periodType=None):
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
                                     join fa_file_data fd on fd.OID = fact.fileDataOID
                                     join fa_company company on fd.companyOID = company.OID
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
                                     join fa_custom_fact_value factValue on factValue.customFactOID = fact.OID
                                     join fa_file_data fd on fd.OID = factValue.fileDataOID
                                     join fa_company company on fd.companyOID = company.OID
                                     join fa_custom_concept concept on fact.customConceptOID = concept.OID
                                     join fa_custom_report report on fact.customReportOID = report.OID
                                     join fa_period period on factValue.periodOID = period.OID
                                 where
                                    (:conceptName is null or concept.conceptName = :conceptName)
                                    and (:ticker is null or company.ticker = :ticker)
                                    and (:reportShortName is null or report.shortName = :reportShortName )
                                    and (:periodType is null or period.type = :periodType )) as rs
                                        order by reportShortName, conceptName, periodType, date_""")
            rs = con.execute(query, params)
            return rs 
        
    def addFact(self, factVOList, fileData, reportDict, session, replace):
        objectAlreadyAdded = {}
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                concept = ConceptEngine().getOrCreateConcept(factVO.conceptName, session)
                factKey = str(concept.OID) + "-" + str(reportDict[factVO.reportRole].OID) + "-" + str(fileData.OID)
                if(objectAlreadyAdded.get(factKey, None) is None):
                    fact = FactDao.getFact(concept, reportDict[factVO.reportRole], fileData, session)
                    if(fact is None):
                        fact = Fact()
                        fact.conceptOID = concept.OID
                        fact.reportOID = reportDict[factVO.reportRole].OID
                        fact.fileDataOID = fileData.OID
                        fact.order_ = factVO.order
#                     if(replace):
#                         for itemToDelete in fact.factValueList:
#                             session.delete(itemToDelete)
                        Dao().addObject(objectToAdd=fact, session=session, doFlush=True)
                    else:
                        for factValue in fact.factValueList:
                            factValuekey = str(factValue.periodOID) + "-" + str(fact.OID)
                            objectAlreadyAdded[factValuekey] = ""
                    
                    for factValueVO in factVO.factValueList:
                        factValuekey = str(factValueVO.period.OID) + "-" + str(fact.OID)
                        if(objectAlreadyAdded.get(factValuekey, None) is None):
                            factValue = FactValue()
                            factValue.value = factValueVO.value
                            factValue.period = factValueVO.period
                            fact.factValueList.append(factValue)
                            objectAlreadyAdded[factValuekey] = ""
                    objectAlreadyAdded[factKey] = "" 
                    logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added fact" + str(factVO.conceptName))
                    # print("STEP 3.1 " + str(datetime.now() - time1))
                    if(len(factVO.factValueList) == 0):
                        logging.getLogger(Constant.LOGGER_NONEFACTVALUE).debug("NoneFactValue " + fact.concept.conceptName + " " + fileData.fileName)
        session.commit()
        
    @staticmethod
    def getFact(concept, report, fileData, session):
        return GenericDao().getOneResult(Fact, and_(Fact.concept == concept, Fact.report == report, Fact.fileData == fileData), session, raiseNoResultFound=False)
    
    def getFactValue2(self, ticker, periodType=None, documentType=None, concept=None, session=None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            objectResult = session.query(FactValue)\
                .join(FactValue.fact)\
                .join(Fact.concept)\
                .join(Fact.report)\
                .join(FactValue.period)\
                .join(Fact.fileData)\
                .join(FileData.company)\
                .filter(and_(Company.ticker.__eq__(ticker), Period.type.__eq__(periodType), \
                             or_(FileData.documentType.__eq__(documentType), documentType == None), \
                             Concept.conceptName.__eq__(concept.conceptName)))\
                .order_by(Period.endDate)\
                .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, Fact.fileDataOID)\
                .distinct()\
                .all()
            return objectResult
        except NoResultFound:
            return FactValue()
        
    def getFactValue3(self, periodType=None, conceptOID=None, fileDataOID=None, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(FactValue)\
            .join(FactValue.fact)\
            .join(FactValue.period)\
            .join(Fact.fileData)\
            .filter(and_(Period.type.__eq__(periodType), \
                         FileData.OID == fileDataOID, \
                         Fact.conceptOID.__eq__(conceptOID)))\
            .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, Fact.fileDataOID)\
            .all()#.distinct()\#.order_by(Period.endDate)\
        return objectResult
    
    def getFactValue4(self, companyOID, periodType, conceptOID, documentFiscalYearFocus, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(FactValue)\
            .join(FactValue.fact)\
            .join(FactValue.period)\
            .join(Fact.fileData)\
            .filter(and_(Period.type.__eq__(periodType), \
                         FileData.documentFiscalYearFocus == documentFiscalYearFocus, \
                         FileData.companyOID == companyOID, \
                         Fact.conceptOID.__eq__(conceptOID)))\
            .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, Fact.fileDataOID)\
            .all()#.distinct()\#.order_by(Period.endDate)\
        return objectResult
