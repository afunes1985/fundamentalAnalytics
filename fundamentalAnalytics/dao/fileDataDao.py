'''
Created on Apr 19, 2019

@author: afunes
'''
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.expression import text, or_, outerjoin
from sqlalchemy.sql.functions import func

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.errorMessage import ErrorMessage
from modelClass.fileData import FileData
from modelClass.ticker import Ticker
from valueobject.constant import Constant
from modelClass.entityFactValue import EntityFactValue


class FileDataDao():

    def getFileDataList3(self, filename='', ticker='', session=None):
        try:
            dbconnector = DBConnector()
            if (session is None): 
                session = dbconnector.getNewSession()
            query = session.query(FileData)\
            .join(FileData.company)\
            .join(Company.tickerList)\
            .with_entities(Company.CIK, Ticker.ticker, FileData.fileName, FileData.documentPeriodEndDate, FileData.documentType, FileData.documentFiscalYearFocus, FileData.documentFiscalPeriodFocus, FileData.fileStatus, FileData.factStatus, FileData.entityStatus, FileData.priceStatus, FileData.copyStatus, FileData.calculateStatus, FileData.expressionStatus)\
            .order_by(FileData.documentPeriodEndDate)\
            .filter(or_(and_(FileData.fileName.like('%' + filename + '%'), filename != ''), and_(Ticker.ticker == ticker, ticker != '')))
            objectResult = query.all()
            return objectResult
        except NoResultFound:
            return None
        
    def addOrModifyFileData(self, statusKey=None, statusValue=None, filename=None, externalSession=None, errorMessage=None, errorKey=None, fileData=None):
        try:
            if (externalSession is None):
                session = DBConnector().getNewSession()
            else:
                session = externalSession
            if (fileData is None):
                fileData = FileDataDao.getFileData(filename, session)
                setattr(fileData, statusKey, statusValue)
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
            .order_by(FileData.documentPeriodEndDate)\
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
    
    def getFileData7(self, session):
        """get FD for entityfactValue without explicit member"""
        dbconnector = DBConnector()
        if (session is None): 
            session = dbconnector.getNewSession()
        query = session.query(func.max(FileData.fileName))\
            .join(FileData.entityFactValueList)\
            .filter(EntityFactValue.explicitMember.isnot(None))\
            .with_entities(FileData.fileName)\
            .group_by(FileData.companyOID)
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
                SELECT fileStatus, if(listed=1,'LISTED','NOT_LISTED') as companyStatus, entityStatus, priceStatus, factStatus, count(*) as values_ 
                    FROM fa_file_data fd
                        join fa_company c on c.oid = fd.companyOID
                    group by fileStatus, listed, entityStatus, priceStatus, factStatus""")
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
