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
from tools.tools import FileNotFoundException, XSDNotFoundException, createLog, \
    XMLNotFoundException
from valueobject.constant import Constant


class AbstractImporter(object):

    cacheDict = {}
    logger = None
    
    def __init__(self, errorKey, filename, replace, previousStatus=None, actualStatus=None, isNullPool=False):
        self.initLogger()
        self.errorKey = errorKey
        self.filename = filename
        self.fileDataDao = FileDataDao()
        self.session = DBConnector(isNullPool=isNullPool).getNewSession()
        self.replace = replace
        self.previousStatus = previousStatus
        self.actualStatus = actualStatus
        self.fileData = FileDataDao.getFileData(self.filename, self.session)
        if(not AbstractImporter.cacheDict):
            self.initCache()
        
    def doImport(self):
        try:
            logging.info("START")
            time1 = datetime.now()
            if(self.skipOrProcess()):
                self.addOrModifyInit()
                if(self.replace):
                    self.deleteImportedObject()
                self.logger.debug("**********START - Processing filename " + self.filename)
                voList = self.doImport2()
                persistentList = self.getPersistentList(voList)
                Dao().addObjectList(persistentList, self.session)
                if(voList is None or len(voList) != 0):
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_OK)
                else: 
                    setattr(self.fileData , self.actualStatus, Constant.STATUS_NO_DATA) 
                Dao().addObject(objectToAdd=self.fileData, session=self.session, doCommit=True)
                self.logger.info("***********FINISH AT " + str(datetime.now() - time1) + " " + self.filename + " objects added " + str(len(voList)))
        except (FileNotFoundException, XSDNotFoundException,XMLNotFoundException) as e:
            self.logger.debug("ERROR " + str(e))
            self.addOrModifyFDError1(e)
        except MemoryError as e:
            self.logger.info("ERROR " + self.filename)
            self.logger.exception(e)
            FileDataDao().addOrModifyFileData(fileStatus=Constant.STATUS_ERROR, filename=self.filename, errorMessage='MemoryError', errorKey=self.errorKey)
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
        if((self.previousStatus is None or (getattr(self.fileData, self.previousStatus) == Constant.STATUS_OK) and getattr(self.fileData, self.actualStatus) != Constant.STATUS_OK) or self.replace == True):
            return True
        else:
            return False  
    
    @abstractmethod
    def getPersistent(self, vo):
        pass
    
    @abstractmethod
    def getPersistentList(self, voList):
        if(voList is not None):
            # customConceptCreated = [cfv.customFact.customConcept.conceptName for cfv in self.fileData.customFactValueList]
            persistentList = []
            for vo in voList:
                # if(vo.customConcept.conceptName not in customConceptCreated):
                    persistentList.append(self.getPersistent(vo))
            return persistentList
        else:
            return []
    
    @abstractmethod
    def initCache(self):
        pass
    
    @abstractmethod
    def deleteImportedObject(self):
        pass
    
    @abstractmethod
    def initLogger(self):
        if (AbstractImporter.logger is None):
            AbstractImporter.logger = createLog(self.__class__.__name__, logging.INFO)
            
    def doCommit(self):
        self.session.commit()
        
    def addOrModifyFDPending(self):
        setattr(self.fileData , self.actualStatus, Constant.STATUS_PENDING)
        Dao().addObject(objectToAdd=self.fileData, session=self.session, doCommit=True)   
