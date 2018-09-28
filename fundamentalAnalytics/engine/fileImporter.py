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


class FileImporter(AbstractFileImporter):

    def __init__(self, filename):
        self.processCache = None
        self.session = DBConnector().getNewSession()
        self.filename = filename
            
    def doImport(self, replace):
        try:
            fileData = self.initFileData(self.filename, self.session)
            if(fileData.status != "OK" and replace != True):
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.processCache = self.initProcessCache(self.filename, self.session)
                fileData = self.completeFileData(fileData, self.processCache, self.filename, self.session)
                reportDict = self.getReportDict(self.processCache, self.session)
                factToAddList = self.getFactByReport(reportDict, self.processCache, self.session)
                factToAddList = self.setFactValues(factToAddList, self.processCache)
                Dao.addFact(factToAddList, self.company, fileData, self.session)
                fileData.status = "OK"
                Dao.addObject(objectToAdd = fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            session2 = DBConnector().getNewSession()
            fileData2 = Dao.getFileData(self.filename, session2)
            if(fileData2 is None):
                fileData2 = FileData()
            fileData2.fileName = self.filename
            fileData2.status = "ERROR"
            Dao.addObject(objectToAdd = fileData2, session = session2, doCommit = True)
            raise e
        
    
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
        documentType = insXMLDict['dei:DocumentType']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("documentType " + documentType)
        amendmentFlag = insXMLDict['dei:AmendmentFlag']['#text']
        amendmentFlag = amendmentFlag.lower() in ("true")
        documentPeriodEndDate = insXMLDict['dei:DocumentPeriodEndDate']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentPeriodEndDate " + documentPeriodEndDate)
        documentFiscalYearFocus = insXMLDict['dei:DocumentFiscalYearFocus']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalYearFocus " + documentFiscalYearFocus)
        documentFiscalPeriodFocus = insXMLDict['dei:DocumentFiscalPeriodFocus']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalPeriodFocus " + documentFiscalPeriodFocus)
        entityCentralIndexKey = insXMLDict['dei:EntityCentralIndexKey']['#text']
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
        

