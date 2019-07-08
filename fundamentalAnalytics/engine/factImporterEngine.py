'''
Created on 19 sep. 2018

@author: afunes
'''
from datetime import datetime
import logging
import traceback

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.entityFactDao import EntityFactDao
from dao.factDao import FactDao
from dao.fileDataDao import FileDataDao
from engine.abstractFileImporter import AbstractFileImporter
from tools.tools import FileNotFoundException, XSDNotFoundException
from valueobject.constant import Constant


class FactImporterEngine(AbstractFileImporter):

    def __init__(self, filename, replace, mainCache, conceptName = None):
        self.processCache = None
        self.session = DBConnector().getNewSession()
        self.filename = filename
        self.replace = replace
        self.mainCache = mainCache
        self.conceptName = conceptName
        self.fileDataDao = FileDataDao()
            
    def doImport(self):
        try:
            time1 = datetime.now()
            logging.info("START")
            fileData = FileDataDao.getFileData(self.filename, self.session)
            if((fileData.status != Constant.STATUS_OK) or self.replace == True):
                self.fileDataDao.addOrModifyFileData(status = Constant.STATUS_INIT, filename = self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.processCache = self.initProcessCache(self.filename, self.session)
                fileData = self.completeFileData(fileData, self.processCache, self.filename, self.session)
                reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
                factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
                factVOList = self.setFactValues(factVOList, self.processCache)
                FactDao().addFact(factVOList, fileData, reportDict, self.session, self.replace)
                if(len(factVOList) != 0):
                    fileData.status = Constant.STATUS_OK
                else: 
                    fileData.status = "NO_DATA"
                Dao().addObject(objectToAdd = fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).info("FINISH AT " + str(datetime.now() - time1))
        except (FileNotFoundException, XSDNotFoundException) as e:
            self.fileDataDao.addOrModifyFileData(status = e.status, filename = self.filename, errorMessage=str(e))
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        except Exception as e:
            self.session.rollback()
            self.fileDataDao.addOrModifyFileData(status = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:99])
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        finally:
            self.session.commit()
            self.session.close()
            
            
    def doImportEntityFactByConcept(self):
        try:
            logging.info("START")
            fileData = self.fileDataDao.getFileData(self.filename, self.session)
            if((fileData.status == Constant.STATUS_OK and fileData.entityStatus != Constant.STATUS_OK)):
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                time1 = datetime.now()
                self.fileDataDao.addOrModifyFileData(entityStatus = Constant.STATUS_INIT, fileData=fileData, externalSession = self.session)
                self.processCache = self.initProcessCache(self.filename, self.session)
                reportDict = self.getReportDict(self.processCache, ["Cover", "Statements"], self.session)
                factVOList = self.getFactByConcept(reportDict, self.processCache, self.conceptName)
                factVOList = self.setFactValues(factVOList, self.processCache)
                factValueAdded = EntityFactDao().addEntityFact(factVOList, self.company, fileData.OID , reportDict, self.session, self.replace)
                if(factValueAdded > 0):
                    self.fileDataDao.addOrModifyFileData(entityStatus = Constant.STATUS_OK, fileData=fileData, errorMessage='',  externalSession = self.session)
                else:
                    self.fileDataDao.addOrModifyFileData(entityStatus = Constant.STATUS_NO_DATA, fileData=fileData, externalSession = self.session)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).info("FINISH AT " + str(datetime.now() - time1))
        except (FileNotFoundException, XSDNotFoundException) as e:
            self.session.rollback()
            self.session.close()
            self.fileDataDao.addOrModifyFileData(entityStatus = e.status, filename = self.filenamea, errorMessage=str(e))
            logging.getLogger(Constant.LOGGER_GENERAL).error("*******************************END - Processing filename " + self.filename + " " + str(e))
            raise e
        except Exception as e:
            self.session.rollback()
            self.session.close()
            self.fileDataDao.addOrModifyFileData(entityStatus = Constant.STATUS_ERROR, filename = self.filename, errorMessage = str(e)[0:99])
            traceback.print_exc()
            logging.getLogger(Constant.LOGGER_GENERAL).error("*******************************END - Processing filename " + " " +str(e)[0:99])
            raise e
        finally:
            self.session.close()
