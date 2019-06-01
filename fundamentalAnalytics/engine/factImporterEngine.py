'''
Created on 19 sep. 2018

@author: afunes
'''
from datetime import datetime
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.factDao import FactDao
from dao.fileDataDao import FileDataDao
from engine.abstractFileImporter import AbstractFileImporter
from tools.tools import FileNotFoundException, XSDNotFoundException
from valueobject.constant import Constant


class FactImporterEngine(AbstractFileImporter):

    def __init__(self, filename, replace, mainCache):
        self.processCache = None
        self.session = DBConnector().getNewSession()
        self.filename = filename
        self.replace = replace
        self.mainCache = mainCache
            
    def doImport(self):
        try:
            fileData = FileDataDao.getFileData(self.filename, self.session)
            if((fileData.status != "OK") or self.replace == True):
                time1 = datetime.now()
                FileDataDao.addOrModifyFileData(status = "INIT", filename = self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.processCache = self.initProcessCache(self.filename, self.session)
                #print("STEP 1 " + str(datetime.now() - time1))
                fileData = self.completeFileData(fileData, self.processCache, self.filename, self.session)
                #print("STEP 2 " + str(datetime.now() - time1))
                reportDict = self.getReportDict(self.processCache, self.session)
                #print("STEP 3 " + str(datetime.now() - time1))
                factVOList = self.getFactByReport(reportDict, self.processCache, self.session)
                #print("STEP 4 " + str(datetime.now() - time1))
                factVOList = self.setFactValues(factVOList, self.processCache)
                #print("STEP 5 " + str(datetime.now() - time1))
                #if(self.replace):
                #    self.session.delete()
                FactDao.addFact(factVOList, self.company, fileData, reportDict, self.session, self.replace)
                #print("STEP 6 " + str(datetime.now() - time1))
                if(len(factVOList) != 0):
                    fileData.status = "OK"
                else:
                    fileData.status = "NO_DATA"
                Dao.addObject(objectToAdd = fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
                logging.getLogger(Constant.LOGGER_GENERAL).info("FINISH AT " + str(datetime.now() - time1))
        except (FileNotFoundException, XSDNotFoundException) as e:
            FileDataDao.addOrModifyFileData(status = e.status, filename = self.filename, errorMessage=str(e))
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        except Exception as e:
            self.session.rollback()
            FileDataDao.addOrModifyFileData(status = "ERROR", filename = self.filename, errorMessage = str(e)[0:99])
            logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************END - Processing filename " + self.filename)
            raise e
        finally:
            self.session.remove()
