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

    def __init__(self, errorKey, filename, replace, previousStatus, actualStatus):
        self.errorKey = errorKey
        self.filename = filename
        self.fileDataDao = FileDataDao()
        self.session = DBConnector().getNewSession()
        self.replace = replace
        self.previousStatus = previousStatus
        self.actualStatus = actualStatus
        
    def doImport(self):
        try:
            logging.info("START")
            time1 = datetime.now()
            self.fileData = FileDataDao.getFileData(self.filename, self.session)
            if(self.skipOrProcess()):
                self.addOrModifyInit()
                logging.getLogger(Constant.LOGGER_GENERAL).debug("*******************************START - Processing filename " + self.filename)
                voList = self.doImport2()
                persistentList = self.getPersistentList(voList)
                Dao().addObjectList(persistentList, self.session)
                if(voList != 0):
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_OK)
                else: 
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_NO_DATA) 
                Dao().addObject(objectToAdd = self.fileData, session = self.session, doCommit = True)
                logging.getLogger(Constant.LOGGER_GENERAL).info("*******************************FINISH AT " + str(datetime.now() - time1) +  " " + self.filename)
        except (FileNotFoundException, XSDNotFoundException) as e:
            logging.getLogger(Constant.LOGGER_GENERAL).debug("ERROR " + str(e))
            self.addOrModifyFDError1(e)
        except Exception as e:
            logging.getLogger(Constant.LOGGER_GENERAL).info("ERROR " + self.filename)
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
        if((getattr(self.fileData, self.previousStatus)  == Constant.STATUS_OK and getattr(self.fileData, self.actualStatus) != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False  
    
    def getPersistentList(self, voList):
        #customConceptCreated = [cfv.customFact.customConcept.conceptName for cfv in self.fileData.customFactValueList]
        persistentList = []
        for vo in voList:
            #if(vo.customConcept.conceptName not in customConceptCreated):
                persistentList.append(self.getPersistent(vo))
        return persistentList