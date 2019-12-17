'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import text, or_

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.errorMessage import ErrorMessage
from modelClass.fileData import FileData
from valueobject.constant import Constant


class FileDataDao():

    @staticmethod   
    def getFileDataList(status=[], session=None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .with_entities(FileData.fileName, FileData.status, FileData.fileStatus)\
            .filter(FileData.status.in_(status))
            # .with_entities(FileData.fileName, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.entityCentralIndexKey, FileData.status)\
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
    @staticmethod   
    def getFileDataList2(status=[], session=None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            params = { 'status' : status}
            
            query = text("""SELECT fd.fileName, fd.documentType, 
                            fd.documentFiscalYearFocus AS fa_file_data_documentFiscalYearFocus, 
                            fd.documentFiscalPeriodFocus AS fa_file_data_documentFiscalPeriodFocus, 
                            fd.entityCentralIndexKey AS fa_file_data_entityCentralIndexKey, c.ticker AS fa_company_ticker, 
                            fd.status AS fa_file_data_status 
                        from fa_file_data fd
                        inner join (select f.fileDataOID, f.companyOID
                            from fa_fact f
                            group by f.fileDataOID, f.companyOID) as b on b.fileDataOID = fd.oid
                        inner join fa_company c on c.oid = b.companyOID 
                        WHERE fd.status NOT IN ('PENDING')""")
            rs = con.execute(query, params)
            return rs 
        
    @staticmethod   
    def getFileDataList3(filename='', ticker='', session=None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .join(FileData.company)\
            .with_entities(Company.ticker, FileData.fileName, FileData.documentPeriodEndDate, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.entityCentralIndexKey, FileData.fileStatus, FileData.status, FileData.entityStatus, FileData.priceStatus, FileData.copyStatus, FileData.calculateStatus, FileData.expressionStatus)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(or_(and_(FileData.fileName.like('%' + filename + '%'), filename != ''), and_(Company.ticker == ticker, ticker != '')))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
    def getFileDataListWithoutConcept(self, ticker, customConceptOID, session=None):
        try:
            dbconnector = DBConnector()
            fd2 = []
            with dbconnector.engine.connect() as con:
                params = { 'ticker' : ticker,
                           'customConceptOID' : customConceptOID}
                
                query = text("""select fd.OID as fileDataOID, fd.documentPeriodEndDate
                                    from fa_file_data fd
                                        join fa_company comp on comp.oid = fd.companyOID
                                    where comp.ticker = :ticker and
                                        fd.OID not in (select fd.OID
                                            from fa_custom_fact_value cfv 
                                                join fa_custom_fact cf on cfv.customFactOID = cf.OID 
                                                join fa_file_data fd on fd.OID = cfv.fileDataOID
                                                join fa_company comp on comp.oid = fd.companyOID
                                            where comp.ticker = :ticker
                                                and cf.customConceptOID = :customConceptOID)""")
                rs = con.execute(query, params)
                for fd in rs:
                    fd2.append(fd.fileDataOID)
            return fd2 
        except NoResultFound:
            return None
    
    def addOrModifyFileData(self, status=None, fileStatus=None, entityStatus=None, priceStatus=None, expressionStatus=None, copyStatus=None, calculateStatus=None, filename=None, externalSession=None, errorMessage=None, errorKey=None, fileData=None):
        try:
            if (externalSession is None):
                session = DBConnector().getNewSession()
            else:
                session = externalSession
            if (fileData is None):
                fileData = FileDataDao.getFileData(filename, session)
            if (fileData is None):
                fileData = FileData()
                fileData.fileName = filename
            if (status is not None):
                fileData.status = status
            if (entityStatus is not None):
                fileData.entityStatus = entityStatus
            if (priceStatus is not None):
                fileData.priceStatus = priceStatus
            if (calculateStatus is not None):
                fileData.calculateStatus = calculateStatus    
            if fileStatus is not None:
                fileData.fileStatus = fileStatus
            if copyStatus is not None:
                fileData.copyStatus = copyStatus
            if expressionStatus is not None:
                fileData.expressionStatus = expressionStatus
            self.setErrorMessage(errorMessage, errorKey, fileData)
            Dao().addObject(objectToAdd=fileData, session=session, doCommit=True)
            if (externalSession is None):
                session.close()
            return fileData
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            raise e
    
    def setErrorMessage(self, errorMessage, errorKey, fileData):
        if (errorMessage is not None):
                    em = ErrorMessage()
                    em.errorKey = errorKey
                    em.errorMessage = errorMessage
                    fileData.errorMessageList.append(em)
        else:
            for em in fileData.errorMessageList:
                if (em.errorKey == errorKey):
                    fileData.errorMessageList.remove(em)
    
    @staticmethod   
    def getFileData(filename, session=None):
        return GenericDao().getOneResult(FileData, and_(FileData.fileName == filename), session, raiseNoResultFound=False)
        
    @staticmethod   
    def getFileDataYearPeriodList():
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            query = text("""select fd.CIK, fd.documentFiscalYearFocus, fd.documentFiscalPeriodFocus 
                    from fa_file_data fd
                    where fd.documentFiscalYearFocus is not null
                    group by fd.CIK, fd.documentFiscalYearFocus, fd.documentFiscalPeriodFocus
                    order by fd.CIK, fd.documentFiscalYearFocus, fd.documentFiscalPeriodFocus""")
            rs = con.execute(query)
            return rs 
        
    def getFileData3(self, statusAttr, statusValue, statusAttr2, statusValue2, ticker='', session=None, limit=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.company)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, getattr(FileData, statusAttr2) == statusValue2, or_(ticker == '', Company.ticker == ticker)))\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    def getFileData2(self, statusAttr, statusValue, ticker='', session=None, limit=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.company)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, or_(ticker == '', Company.ticker == ticker)))\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    def getErrorList(self, fileName, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.errorMessageList)\
            .order_by(ErrorMessage.errorKey)\
            .filter(FileData.fileName == fileName)\
            .with_entities(ErrorMessage.errorKey, ErrorMessage.errorMessage)
        return query.all()  
    
    def getStatusCount(self):
        session = DBConnector().getNewSession()
        query = text("""
                        SELECT null as parent, null as id, fileStatus as label, count(*) as values_ 
                                FROM fundamentalanalytics.fa_file_data
                                group by fileStatus
                        union                                
                        SELECT fileStatus as parent, CONCAT(fileStatus, " - ", status) as id, status as label, count(*) as values_ 
                                FROM fundamentalanalytics.fa_file_data
                                group by fileStatus, status""")
        return session.execute(query, '')
    
    def getStatusCount2(self):
        session = DBConnector().getNewSession()
        query = text("""
                SELECT fileStatus, status, entityStatus, count(*) as values_ 
                    FROM fundamentalanalytics.fa_file_data
                    group by fileStatus, status""")
        return session.execute(query, '')
        
