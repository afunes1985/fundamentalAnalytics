'''
Created on Jul 8, 2019

@author: afunes
'''
from abc import abstractmethod
from datetime import datetime
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.fileDataDao import FileDataDao
from tools.tools import FileNotFoundException, XSDNotFoundException
from valueobject.constant import Constant


class AbstractImporter(object):

    def __init__(self, errorKey, filename, replace):
        self.errorKey = errorKey
        self.filename = filename
        self.fileDataDao = FileDataDao()
        self.session = DBConnector().getNewSession()
        self.replace = replace
        
    def doImport(self):
        try:
            logging.info("START")
            time1 = datetime.now()
            self.fileData = FileDataDao.getFileData(self.filename, self.session)
            if(self.skipOrProcess()):
                self.addOrModifyInit()
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                self.doImport2()
                Dao().addObject(objectToAdd = self.fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).info("*******************************FINISH AT " + str(datetime.now() - time1) +  " " + self.filename)
        except (FileNotFoundException, XSDNotFoundException) as e:
            logging.getLogger(Constant.LOGGER_GENERAL).debug("ERROR " + str(e))
            self.addOrModifyFDError1(e)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).exception(e)
            self.session.rollback()
            self.addOrModifyFDError2(e)
        finally:
            self.session.commit()
            self.session.close()
    
    @abstractmethod
    def addOrModifyInit(self):
        pass
    
    @abstractmethod
    def addOrModifyFDError1(self, e):
        pass
    
    @abstractmethod
    def addOrModifyFDError2(self, e):
        pass
            
    @abstractmethod
    def doImport2(self):
        pass
    
    @abstractmethod
    def skipOrProcess(self):
        pass
    
