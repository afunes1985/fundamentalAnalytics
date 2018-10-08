'''
Created on 19 sep. 2018

@author: afunes
'''
from datetime import datetime
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from engine.abstractFileImporter import AbstractFileImporter
from modelClass.fileData import FileData
from tools.tools import FileNotFoundException, \
    addOrModifyFileData
from valueobject.constant import Constant


class FileImporter(AbstractFileImporter):

    def __init__(self, filename, replace, mainCache, s):
        self.processCache = None
        self.session = DBConnector().getNewSession()
        self.filename = filename
        self.semaphore = s
        self.replace = replace
        self.mainCache = mainCache
            
    def doImport(self):
        try:
            fileData = Dao.getFileData(self.filename, self.session)
            if((fileData.status != "FNF" and fileData.status != "OK") or self.replace == True):
                time1 = datetime.now()
                addOrModifyFileData(status = "INIT", filename = self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.processCache = self.initProcessCache(self.filename, self.session)
                print("STEP 1 " + str(datetime.now() - time1))
                fileData = self.completeFileData(fileData, self.processCache, self.filename, self.session)
                print("STEP 2 " + str(datetime.now() - time1))
                reportDict = self.getReportDict(self.processCache, self.session)
                print("STEP 3 " + str(datetime.now() - time1))
                factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
                print("STEP 4 " + str(datetime.now() - time1))
                factVOList = self.setFactValues(factVOList, self.processCache)
                print("STEP 5 " + str(datetime.now() - time1))
                Dao.addFact(factVOList, self.company, fileData, self.session)
                print("STEP 6 " + str(datetime.now() - time1))
                fileData.status = "OK"
                Dao.addObject(objectToAdd = fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).info("FINISH AT " + str(datetime.now() - time1))
        except FileNotFoundException as e:
            addOrModifyFileData(status = "FNF", filename = self.filename)
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        except Exception as e:
            addOrModifyFileData(status = "ERROR", filename = self.filename)
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        finally:
            self.session.close()
            self.semaphore.release()
        
    
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
        

