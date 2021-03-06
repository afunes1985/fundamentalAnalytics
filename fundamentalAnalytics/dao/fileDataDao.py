'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import text, or_, outerjoin
from sqlalchemy.sql.functions import func

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.entityFactValue import EntityFactValue
from modelClass.errorMessage import ErrorMessage
from modelClass.fileData import FileData
from modelClass.period import QuarterPeriod
from modelClass.ticker import Ticker
from tools import tools
from valueobject.constant import Constant


class FileDataDao():

    def getFileDataList3(self, filename, session=None):
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .join(FileData.company)\
            .join(Company.tickerList)\
            .with_entities(Company.CIK, Ticker.ticker, FileData.fileName, FileData.documentPeriodEndDate, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.fileStatus, FileData.companyStatus, FileData.entityStatus, FileData.priceStatus, FileData.factStatus, FileData.copyStatus, FileData.calculateStatus, FileData.expressionStatus)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(FileData.fileName.like('%' + filename + '%'))
            objectResult = query.all()
            return objectResult
        
    def getFileDataList4(self, ticker, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
        .join(FileData.company)\
        .join(Company.tickerList)\
        .with_entities(Company.CIK, Ticker.ticker, FileData.fileName, FileData.documentPeriodEndDate, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.fileStatus, FileData.companyStatus, FileData.entityStatus, FileData.priceStatus, FileData.factStatus, FileData.copyStatus, FileData.calculateStatus, FileData.expressionStatus)\
        .order_by(FileData.documentPeriodEndDate)\
        .filter(Ticker.ticker == ticker)
        objectResult = query.all()
        return objectResult
        
    def addOrModifyFileData(self, statusKey=None, statusValue=None, filename=None, externalSession=None, 
                            errorMessage=None, errorKey=None, fileData=None, extraData=None):
        try:
            if (externalSession is None):
                session = DBConnector().getNewSession()
            else:
                session = externalSession
            if (fileData is None):
                fileData = FileDataDao.getFileData(filename, session)
                setattr(fileData, statusKey, statusValue)
            self.setErrorMessage(errorMessage=errorMessage, errorKey=errorKey, extraData=extraData, fileData=fileData)
            Dao().addObject(objectToAdd=fileData, session=session, doCommit=True)
            if (externalSession is None):
                session.close()
            return fileData
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            raise e
    
    def setErrorMessage(self, errorMessage, errorKey, fileData, extraData=None):
        if (errorMessage is not None and errorMessage != ''):
            self.deleteErrorMessage(fileData, errorKey)
            em = ErrorMessage()
            em.errorKey = errorKey
            em.errorMessage = errorMessage
            em.extraData = extraData
            fileData.errorMessageList.append(em)
        else:
            self.deleteErrorMessage(fileData, errorKey)
    
    def deleteErrorMessage(self, fileData, errorKey):
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
        
    def getFileData3(self, statusAttr, statusValue, statusAttr2, statusValue2, session=None, limit=None, listed = ''):
        """get FD by two attributes and listed"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .join(FileData.company)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, getattr(FileData, statusAttr2) == statusValue2, 
                         or_(listed == '', Company.listed == listed)))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    def getFileData4(self, statusAttr, statusValue, statusAttr2, statusValue2, session=None, limit=None):
        """get FD by two attributes"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, getattr(FileData, statusAttr2) == statusValue2))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult

    def getFileData5(self, statusAttr, statusValue, session=None, limit=None, errorMessage2 = ''):
        """get FD by one attribute and error message"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.errorMessageList)\
            .order_by(FileData.documentPeriodEndDate.desc())\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, or_(errorMessage2 == '', ErrorMessage.errorMessage.like(errorMessage2))))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    
    def getFileData6(self, statusAttr, statusValue, session=None, limit=None):
        """get FD by one attribute"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    def getFileData7(self, statusAttr, statusValue, session=None, limit=None):
        """get FD by one attribute"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.errorMessageList)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(and_(getattr(FileData, statusAttr) == statusValue, ErrorMessage.errorMessage.is_(None)))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult
    
    def getLastFileData(self, session):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(func.max(FileData.fileName))\
            .with_entities(FileData.fileName)\
            .group_by(FileData.companyOID)
        objectResult = query.all()
        return objectResult
    
    def getLastFileDataByCIK(self, CIK, session):
        session = DBConnector().getNewSession()
        params = { 'CIK' : CIK}
        query = text("""
                    select fileName 
                    from fa_file_data fd
                    join fa_company  c on fd.companyOID = c.OID
                    where CIK = :CIK and 
                    documentPeriodEndDate = (
                        select max(documentPeriodEndDate) 
                        from fa_file_data fd
                        join fa_company  c on fd.companyOID = c.OID 
                        where CIK = :CIK
                        group by companyOID);
                    """)
        return session.execute(query, params)
    
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
    
    def getStatusCount2(self):
        session = DBConnector().getNewSession()
        query = text("""
                    SELECT fileStatus, companyStatus, if(listed=1,'LISTED','NOT_LISTED') as listedStatus, entityStatus, priceStatus, factStatus, count(*) as values_ 
                    FROM fa_file_data fd
                        join fa_company c on c.oid = fd.companyOID
                    group by fileStatus, companyStatus, listed, entityStatus, priceStatus, factStatus""")
        return session.execute(query, '')
    
    def getStatusCount3(self):
        session = DBConnector().getNewSession()
        query = text("""
                SELECT factStatus, copyStatus, calculateStatus, expressionStatus, count(*) as values_ 
                    FROM fa_file_data fd
                        join fa_company c on c.oid = fd.companyOID
                    where c.listed = 1
                    group by factStatus, copyStatus, calculateStatus, expressionStatus""")
        return session.execute(query, '')
    
    def getErrorMessageGroup(self, errorKey, statusKey, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(ErrorMessage)\
            .join(ErrorMessage.fileData)\
            .filter(ErrorMessage.errorKey == errorKey)\
            .with_entities(getattr(FileData, statusKey), ErrorMessage.errorMessage, func.count().label('Count'))\
            .order_by(ErrorMessage.errorMessage)\
            .group_by(FileData.companyStatus, ErrorMessage.errorMessage)
        objectResult = query.all()
        return objectResult
    
    def getErrorKeyList(self, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(ErrorMessage)\
            .with_entities(ErrorMessage.errorKey.distinct())
        objectResult = query.all()
        return objectResult


    def getStatusList(self, statusAttr, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .with_entities(getattr(FileData, statusAttr).distinct())
        objectResult = query.all()
        return objectResult
    
    def getQuarterPeriodList(self, session=None):
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(QuarterPeriod)\
            .with_entities(QuarterPeriod.OID, functions.concat(QuarterPeriod.year, '-' ,QuarterPeriod.quarter))\
            .order_by(QuarterPeriod.year.desc(), QuarterPeriod.quarter.desc())
        objectResult = query.all()
        return objectResult
    
    def getQuarterPeriod(self, quarterPeriodOID, session=None):
        return GenericDao().getOneResult(objectClazz=QuarterPeriod, condition=(QuarterPeriod.OID==quarterPeriodOID), session=session, raiseNoResultFound=True)
    
    def getFileDataForReport(self, fileStatus, companyStatus, entityFactStatus, priceStatus, factStatus, copyStatus, calculateStatus, expressionStatus, session=None, limit=None):
        """get FD for Report"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.errorMessageList)\
            .filter(and_(or_(fileStatus == None, FileData.fileStatus == fileStatus),
                        or_(companyStatus == None, FileData.companyStatus == companyStatus),
                        or_(entityFactStatus == None, FileData.entityStatus == entityFactStatus),
                        or_(priceStatus == None, FileData.priceStatus == priceStatus),
                        or_(factStatus == None, FileData.factStatus == factStatus),
                        or_(copyStatus == None, FileData.copyStatus == copyStatus),
                        or_(calculateStatus == None, FileData.calculateStatus == calculateStatus),
                        or_(expressionStatus == None, FileData.expressionStatus == expressionStatus)))\
            .with_entities(FileData.fileName, FileData.fileStatus, FileData.companyStatus, FileData.entityStatus, FileData.priceStatus, FileData.factStatus, FileData.copyStatus, FileData.calculateStatus, FileData.expressionStatus, ErrorMessage.errorKey, ErrorMessage.errorMessage)\
            .limit(limit)
        #tools.showQuery(query, dbconnector) 
        objectResult = query.all()
        return objectResult
    
    def getFileDataByError(self, errorKey, errorMessage, session=None, limit=None):
        """get FD by error key and error message"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(FileData)\
            .outerjoin(FileData.errorMessageList)\
            .order_by(FileData.documentPeriodEndDate.desc())\
            .filter(and_(ErrorMessage.errorKey== errorKey, ErrorMessage.errorMessage.like(errorMessage)))\
            .with_entities(FileData.fileName)\
            .limit(limit)
        objectResult = query.all()
        return objectResult