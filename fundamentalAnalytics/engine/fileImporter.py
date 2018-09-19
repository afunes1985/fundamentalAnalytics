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
        logging.getLogger('general').debug("START - Processing filename " + self.filename)
        fileData = self.getFileData(self.processCache, self.filename, self.session)
        reportDict = self.getReportDict(self.processCache, self.session)
        factToAddList = self.getFactByReport(reportDict, self.processCache, self.session)
        factToAddList = self.setFactValues(factToAddList, self.processCache)
        Dao.addFact(factToAddList, self.company, fileData, self.session)
        #for factVO in factToAddList:
            #logging.getLogger('general').debug(factVO.report.shortName + " " + factVO.conceptName + " " + str(factVO.factValueList)) 
        logging.getLogger('general').debug("END - Processing filename " + self.filename)
        
        
    def getFileData(self, processCache, filename, session):
        insXMLDict = processCache[Constant.DOCUMENT_INS]
        periodDict = processCache[Constant.PERIOD_DICT]
        documentType = insXMLDict['dei:DocumentType']['#text']
        logging.getLogger('general').debug("documentType " + documentType)
        amendmentFlag = insXMLDict['dei:AmendmentFlag']['#text']
        amendmentFlag = amendmentFlag.lower() in ("true")
        documentPeriodEndDate = insXMLDict['dei:DocumentPeriodEndDate']['#text']
        logging.getLogger('general').debug("DocumentPeriodEndDate " + documentPeriodEndDate)
        documentFiscalYearFocus = insXMLDict['dei:DocumentFiscalYearFocus']['#text']
        logging.getLogger('general').debug("DocumentFiscalYearFocus " + documentFiscalYearFocus)
        documentFiscalPeriodFocus = insXMLDict['dei:DocumentFiscalPeriodFocus']['#text']
        logging.getLogger('general').debug("DocumentFiscalPeriodFocus " + documentFiscalPeriodFocus)
        entityCentralIndexKey = insXMLDict['dei:EntityCentralIndexKey']['#text']
        logging.getLogger('general').debug("EntityCentralIndexKey " + entityCentralIndexKey)
        #tradingSymbol = insXMLDict['dei:TradingSymbol']['#text']
        #logging.getLogger('general').debug("TradingSymbol " + tradingSymbol)
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
        logging.getLogger('addToDB').debug("Added fileData" + fileData.documentPeriodEndDate)
        return fileData
        

