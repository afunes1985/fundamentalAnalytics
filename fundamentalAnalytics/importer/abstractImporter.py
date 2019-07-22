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
from tools.tools import FileNotFoundException, XSDNotFoundException, createLog
from valueobject.constant import Constant


class AbstractImporter(object):

    cacheDict = {}
    logger = None
    
    def __init__(self, errorKey, filename, replace, previousStatus, actualStatus):
        self.initLogger()
        self.errorKey = errorKey
        self.filename = filename
        self.fileDataDao = FileDataDao()
        self.session = DBConnector().getNewSession()
        self.replace = replace
        self.previousStatus = previousStatus
        self.actualStatus = actualStatus
        if(not AbstractImporter.cacheDict):
            self.initCache()
        
        
    def doImport(self):
        try:
            logging.info("START")
            time1 = datetime.now()
            self.fileData = FileDataDao.getFileData(self.filename, self.session)
            if(self.skipOrProcess()):
                self.addOrModifyInit()
                self.logger.debug("**********START - Processing filename " + self.filename)
                voList = self.doImport2()
                persistentList = self.getPersistentList(voList)
                Dao().addObjectList(persistentList, self.session)
                if(voList != 0):
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_OK)
                else: 
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_NO_DATA) 
                Dao().addObject(objectToAdd = self.fileData, session = self.session, doCommit = True)
                self.logger.info("***********FINISH AT " + str(datetime.now() - time1) +  " " + self.filename)
        except (FileNotFoundException, XSDNotFoundException) as e:
            self.logger.debug("ERROR " + str(e))
            self.addOrModifyFDError1(e)
        except Exception as e:
            self.logger.info("ERROR " + self.filename)
            self.logger.exception(e)
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
    
    @abstractmethod
    def getPersistent(self, vo):
        pass
    
    @abstractmethod
    def getPersistentList(self, voList):
        #customConceptCreated = [cfv.customFact.customConcept.conceptName for cfv in self.fileData.customFactValueList]
        persistentList = []
        for vo in voList:
            #if(vo.customConcept.conceptName not in customConceptCreated):
                persistentList.append(self.getPersistent(vo))
        return persistentList
    
    @abstractmethod
    def initCache(self):
        pass
    
    @abstractmethod
    def initLogger(self):
        if (AbstractImporter.logger is None):
            AbstractImporter.logger = createLog(self.__class__.__name__, logging.INFO)