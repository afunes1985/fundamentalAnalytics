'''
Created on 19 sep. 2018

@author: afunes
'''
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from engine.abstractFileImporter import AbstractFileImporter
from valueobject.constant import Constant


class FileImporter(AbstractFileImporter):

    def __init__(self, filename, replace):
        self.session = DBConnector().getNewSession()
        self.processCache = self.initProcessCache(filename, self.session)
        self.filename = filename
            
    def doImport(self):
        try:
            logging.getLogger(Constant.LOGGER_GENERAL).debug("START - Processing filename " + self.filename)
            fileData = self.getFileData(self.processCache, self.filename, self.session)
            reportDict = self.getReportDict(self.processCache, self.session)
            factToAddList = self.getFactByReport(reportDict, self.processCache, self.session)
            factToAddList = self.setFactValues(factToAddList, self.processCache)
            Dao.addFact(factToAddList, self.company, fileData, self.session)
            logging.getLogger(Constant.LOGGER_GENERAL).debug("END - Processing filename " + self.filename)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_ERROR).debug("ERROR " + self.filename + " " + str(e))
        
        
    def getFileData(self, processCache, filename, session):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        documentType = insXMLDict['dei:DocumentType']['#text']
        logging.getLogger(Constant.LOGGER_GENERAL).debug("documentType " + documentType)
        amendmentFlag = insXMLDict['dei:AmendmentFlag']['#text']
        amendmentFlag = amendmentFlag.lower() in ("true")
        documentPeriodEndDate = insXMLDict['dei:DocumentPeriodEndDate']['#text']
        logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentPeriodEndDate " + documentPeriodEndDate)
        documentFiscalYearFocus = insXMLDict['dei:DocumentFiscalYearFocus']['#text']
        logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalYearFocus " + documentFiscalYearFocus)
        documentFiscalPeriodFocus = insXMLDict['dei:DocumentFiscalPeriodFocus']['#text']
        logging.getLogger(Constant.LOGGER_GENERAL).debug("DocumentFiscalPeriodFocus " + documentFiscalPeriodFocus)
        entityCentralIndexKey = insXMLDict['dei:EntityCentralIndexKey']['#text']
        logging.getLogger(Constant.LOGGER_GENERAL).debug("EntityCentralIndexKey " + entityCentralIndexKey)
        #tradingSymbol = insXMLDict['dei:TradingSymbol']['#text']
        #logging.getLogger(Constant.LOGGER_GENERAL).debug("TradingSymbol " + tradingSymbol)
        #entityRegistrantName = insXMLDict['dei:EntityRegistrantName']['#text']
        fileData = Dao.getFileData(documentPeriodEndDate, entityCentralIndexKey, session)
        fileData.documentType = documentType
        fileData.amendmentFlag = amendmentFlag
        fileData.documentPeriodEndDate = documentPeriodEndDate
        fileData.documentFiscalYearFocus = documentFiscalYearFocus
        fileData.documentFiscalPeriodFocus = documentFiscalPeriodFocus
        fileData.entityCentralIndexKey = entityCentralIndexKey
        fileData.fileName = filename
        #fileData.tradingSymbol = tradingSymbol
        #fileData.entityRegistrantName = entityRegistrantName
        session.add(fileData)
        session.flush()
        logging.getLogger(Constant.LOGGER_ADDTODB).debug("Added fileData" + fileData.documentPeriodEndDate)
        return fileData
        

