'''
Created on Apr 19, 2019

@author: afunes
'''
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import text

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.fileData import FileData


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
            .with_entities(FileData.fileName, FileData.documentPeriodEndDate,  FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.entityCentralIndexKey, FileData.status)\
            .filter(FileData.fileName.like('%' + filename + '%'))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
    
    @staticmethod     
    def addOrModifyFileData(status, importStatus = None, filename = None, externalSession = None, errorMessage = None):
        if (externalSession is None):
            session = DBConnector().getNewSession()
        else:
            session = externalSession
        fileData = FileDataDao.getFileData(filename, session)
        if (fileData is None):
            fileData = FileData()
            fileData.fileName = filename
        fileData.status = status
        fileData.errorMessage = errorMessage
        if importStatus is not None:
            fileData.importStatus = importStatus
        Dao.addObject(objectToAdd = fileData, session = session, doCommit = True)
        if (externalSession is None):
            session.close()
        return fileData
    
    @staticmethod   
    def getFileData(filename, session = None):
        try:
            return GenericDao.getOneResult(FileData, and_(FileData.fileName == filename), session)
        except NoResultFound:
            return None
    