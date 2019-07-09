'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import text, outerjoin

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.errorMessage import ErrorMessage
from modelClass.fileData import FileData
from valueobject.constant import Constant


class FileDataDao():
    @staticmethod   
    def getFileDataList(status = [], session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .with_entities(FileData.fileName,FileData.status, FileData.importStatus)\
            .filter(FileData.status.in_(status))
            #.with_entities(FileData.fileName, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.entityCentralIndexKey, FileData.status)\
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
        
        
    @staticmethod   
    def getFileDataList2(status = [], session = None):
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
    def getFileDataList3(filename = None, session = None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .with_entities(FileData.fileName, FileData.documentPeriodEndDate,  FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.entityCentralIndexKey, FileData.status, FileData.importStatus, FileData.entityStatus, FileData.priceStatus)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(FileData.fileName.like('%' + filename + '%'))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
    @staticmethod   
    def getFileDataListWithoutConcept(ticker, customConceptOID, session = None):
        try:
            dbconnector = DBConnector()
            with dbconnector.engine.connect() as con:
                params = { 'ticker' : ticker,
                           'customConceptOID' : customConceptOID}
                
                query = text("""select fd.OID as fileDataOID
                                    from fa_file_data fd
                                        join fa_company comp on comp.oid = fd.companyOID
                                        left join fa_custom_fact fact on fd.oid = fact.fileDataOID and fact.customConceptOID = :customConceptOID
                                    where comp.ticker = :ticker
                                        and fact.OID is null""")
                rs = con.execute(query, params)
            return rs 
        except NoResultFound:
            return None
    
    def addOrModifyFileData(self, status = None, importStatus = None, entityStatus = None, priceStatus = None, filename = None, externalSession = None, errorMessage = None, errorKey = None, fileData = None):
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
                if (errorMessage is not None and errorKey == Constant.ERROR_KEY_FACT):
                    em = ErrorMessage()
                    em.errorKey = errorKey
                    em.errorMessage = errorMessage
                    fileData.errorMessageList.append(em)
                else:
                    for em in fileData.errorMessageList:
                        if (em.errorKey == errorKey):
                            fileData.errorMessageList.remove(em)
            if (entityStatus is not None):
                fileData.entityStatus = entityStatus
                if (errorMessage is not None and errorKey == Constant.ERROR_KEY_ENTITY_FACT):
                    em = ErrorMessage()
                    em.errorKey = errorKey
                    em.errorMessage = errorMessage
                    fileData.errorMessageList.append(em)
                else:
                    for em in fileData.errorMessageList:
                        if (em.errorKey == errorKey):
                            fileData.errorMessageList.remove(em)
            if (priceStatus is not None):
                fileData.priceStatus = priceStatus    
            if importStatus is not None:
                fileData.importStatus = importStatus
                if (errorMessage is not None and errorKey == Constant.ERROR_KEY_FILE):
                    em = ErrorMessage()
                    em.errorKey = errorKey
                    em.errorMessage = errorMessage
                    fileData.errorMessageList.append(em)
                else:
                    fileData.errorMessageList = []
                    #for em in fileData.errorMessageList:
                    #    if (em.errorKey == 'FILE_ERROR'):
                    #        fileData.errorMessageList.remove(em)
            Dao().addObject(objectToAdd = fileData, session = session, doCommit = True)
            if (externalSession is None):
                session.close()
            return fileData
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            raise e
    
    @staticmethod   
    def getFileData(filename, session = None):
        return GenericDao.getOneResult(FileData, and_(FileData.fileName == filename), session, raiseNoResultFound = False)
        
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
    