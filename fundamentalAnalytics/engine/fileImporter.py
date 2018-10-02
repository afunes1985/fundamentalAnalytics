'''
Created on 19 sep. 2018

@author: afunes
'''
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from engine.abstractFileImporter import AbstractFileImporter
from valueobject.constant import Constant
from modelClass.fileData import FileData
from tools.tools import FileNotFoundException


class FileImporter(AbstractFileImporter):

    def __init__(self, filename):
        self.processCache = None
        self.session = DBConnector().getNewSession()
        self.filename = filename
            
    def doImport(self, replace):
        try:
            fileData = self.initFileData(self.filename, self.session)
            if(fileData.status != "FNF" and fileData.status != "OK" and replace != True):
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.processCache = self.initProcessCache(self.filename, self.session)
                fileData = self.completeFileData(fileData, self.processCache, self.filename, self.session)
                reportDict = self.getReportDict(self.processCache, self.session)
                factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
                factVOList = self.setFactValues(factVOList, self.processCache)
                Dao.addFact(factVOList, self.company, fileData, self.session)
                fileData.status = "OK"
                Dao.addObject(objectToAdd = fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
        except FileNotFoundException as e:
            self.addFileDataNewTrx("FNF")
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        except Exception as e:
            self.addFileDataNewTrx("ERROR")
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        finally:
            self.session.close()
        
    
    def addFileDataNewTrx(self, status):
        session2 = DBConnector().getNewSession()
        fileData2 = Dao.getFileData(self.filename, session2)
        if(fileData2 is None):
            fileData2 = FileData()
        fileData2.fileName = self.filename
        fileData2.status = status
        Dao.addObject(objectToAdd = fileData2, session = session2, doCommit = True)
        session2.close()
    
    def initFileData(self, filename, session):
        fileData = Dao.getFileData(filename, session)
        if (fileData is None):
            fileData = FileData()
            fileData.fileName = filename
            fileData.status = "PEND"
            Dao.addObject(objectToAdd = fileData, doCommit = True)
        return fileData
    
    def completeFileData(self, fileData, processCache, filename, session):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        documentType = self.getValueFromElement(['#text'], insXMLDict['dei:DocumentType'])
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("documentType " + documentType)
        amendmentFlag = self.getValueFromElement(['#text'], insXMLDict['dei:AmendmentFlag'])
        amendmentFlag = amendmentFlag.lower() in ("true")
        documentPeriodEndDate = self.getValueFromElement(['#text'], insXMLDict['dei:DocumentPeriodEndDate'])
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentPeriodEndDate " + documentPeriodEndDate)
        documentFiscalYearFocus = self.getValueFromElement(['#text'], insXMLDict['dei:DocumentFiscalYearFocus'])
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalYearFocus " + documentFiscalYearFocus)
        documentFiscalPeriodFocus = self.getValueFromElement(['#text'], insXMLDict['dei:DocumentFiscalPeriodFocus'])
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalPeriodFocus " + documentFiscalPeriodFocus)
        entityCentralIndexKey = self.getValueFromElement(['#text'], insXMLDict['dei:EntityCentralIndexKey'])
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("EntityCentralIndexKey " + entityCentralIndexKey)
        #tradingSymbol = insXMLDict['dei:TradingSymbol']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("TradingSymbol " + tradingSymbol)
        #entityRegistrantName = insXMLDict['dei:EntityRegistrantName']['#text']
        fileData.documentType = documentType
        fileData.amendmentFlag = amendmentFlag
        fileData.documentPeriodEndDate = documentPeriodEndDate
        fileData.documentFiscalYearFocus = documentFiscalYearFocus
        fileData.documentFiscalPeriodFocus = documentFiscalPeriodFocus
        fileData.entityCentralIndexKey = entityCentralIndexKey
        #fileData.tradingSymbol = tradingSymbol
        #fileData.entityRegistrantName = entityRegistrantName
        #Dao.addObject(objectToAdd = fileData, session = session, doCommit = True)
        return fileData
        

