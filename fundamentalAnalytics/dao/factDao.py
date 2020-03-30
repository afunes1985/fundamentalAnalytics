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
from modelClass.fact import Fact, RelFactReport
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
                                    left join fa_ticker t on t.companyOID = company.OID
                                where concept.conceptName = :conceptName
                                    and (company.CIK = :CIK or :CIK is null)
                                    and (t.ticker = :ticker or :ticker is null)
                                    and (period.type= :periodType or :periodType is null or period.type is null)
                                order by IFNULL(period.endDate, period.instant)""")
            rs = con.execute(query, params)
            return rs 
        
    @staticmethod
    def getFactValues2(CIK, customOrFact = 'Both', reportShortName=None, conceptName=None, periodType=None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'conceptName' : conceptName,
                       'CIK' : CIK,
                       'reportShortName' : reportShortName,
                       'periodType' : periodType,
                       'customOrFact' : customOrFact}
            
            query = text("""select * from(
                            select report.shortName as reportShortName, concept.conceptName, concept.label, factValue.value, 
                                    IFNULL(period.endDate, period.instant) date_, period.type as periodType, frf.order_ as order_
                                 FROM fa_fact fact
                                     join fa_file_data fd on fd.OID = fact.fileDataOID
                                     join fa_company company on fd.companyOID = company.OID
                                     join fa_concept concept on fact.conceptOID = concept.OID
                                     join fa_fact_report_relation frf on frf.factOID =fact.OID
                                     join fa_report report on frf.reportOID = report.OID
                                     join fa_fact_value factValue on factValue.OID = fact.factValueOID
                                     join fa_period period on factValue.periodOID = period.OID
                                 where 
                                    (:conceptName is null or concept.conceptName = :conceptName)
                                    and (:reportShortName is null or report.shortName = :reportShortName )
                                    and (:periodType is null or period.type = :periodType )
                                    and (:CIK is null or company.CIK = :CIK )
                                    and (:customOrFact = 'Both' or :customOrFact = 'Fact')
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
                                    and (:reportShortName is null or report.shortName = :reportShortName )
                                    and (:periodType is null or period.type = :periodType )
                                    and (:CIK is null or company.CIK = :CIK)
                                    and (:customOrFact = 'Both' or :customOrFact = 'Custom')) as rs
                                        order by reportShortName, conceptName, periodType, date_""")
            rs = con.execute(query, params)
            return rs 
        
    def addFact(self, factVOList, fileData, reportDict, session):
        objectAlreadyAdded = {}
        factAlreadyAdded = {}
        for factVO in factVOList:
            if (len(factVO.factValueList) > 0):
                concept = ConceptEngine().getOrCreateConcept(factVO.conceptName, session)
                for factValueVO in factVO.factValueList:
                    factValuekey = "FV-"+str(factValueVO.period.OID) + "-" + str(concept.OID)
                    factValue = objectAlreadyAdded.get(factValuekey, None)
                    if(factValue is None):
                        factValue = FactValue()
                        factValue.value = factValueVO.value
                        factValue.period = factValueVO.period
                        objectAlreadyAdded[factValuekey] = factValue

                    factKey = "F-"+str(concept.OID)+"-"+str(reportDict[factVO.reportRole].OID)+"-"+str(factValueVO.period.OID)
                    if(factAlreadyAdded.get(factKey, None)is None):
                        fact = Fact()
                        fact.conceptOID = concept.OID
                        fact.fileDataOID = fileData.OID
                        frRelation = RelFactReport()
                        frRelation.reportOID = reportDict[factVO.reportRole].OID
                        frRelation.order_ = factVO.order
                        fact.relationReportList.append(frRelation)
                        factValue.factList.append(fact)
                        factAlreadyAdded[factKey] = ""
        for fv in objectAlreadyAdded.values():
            Dao().addObject(objectToAdd=fv, session=session)            
        session.commit()

        
    def getFact(self, concept, fileData, session):
        return GenericDao().getOneResult(Fact, and_(Fact.concept == concept, Fact.fileData == fileData), session, raiseNoResultFound=False)
    
    def getFactValue3(self, periodTypeList=None, fileDataOID=None, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(FactValue)\
            .join(FactValue.factList)\
            .join(FactValue.period)\
            .join(Fact.fileData)\
            .filter(and_(Period.type.in_(periodTypeList), \
                         FileData.OID == fileDataOID))\
            .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, Fact.fileDataOID, Fact.conceptOID, Period.type)\
            .all()#.distinct()\#.order_by(Period.endDate)\
        return objectResult
    
    def getFactValue4(self, companyOID, periodType, documentFiscalYearFocus, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        objectResult = session.query(FactValue)\
            .join(FactValue.factList)\
            .join(FactValue.period)\
            .join(Fact.fileData)\
            .filter(and_(Period.type.__eq__(periodType), \
                         FileData.documentFiscalYearFocus == documentFiscalYearFocus, \
                         FileData.companyOID == companyOID))\
            .with_entities(FactValue.value, FactValue.periodOID, Period.endDate, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, Fact.fileDataOID, Fact.conceptOID)\
            .order_by(Period.endDate)\
            .all()#\#.order_by(Period.endDate).distinct()\
        return objectResult
    
    def deleteFactByFD(self, fileDataOID, session = None):
        params = { 'fileDataOID' : fileDataOID}
        query = text("""    DELETE fa_fact, fa_fact_value, fa_fact_report_relation
                                FROM fa_fact
                                INNER JOIN fa_fact_value ON fa_fact.factValueOID = fa_fact_value.OID
                                INNER JOIN fa_fact_report_relation ON fa_fact_report_relation.factOID = fa_fact.OID
                                WHERE fa_fact.fileDataOID = :fileDataOID""")
        session.execute(query, params)
