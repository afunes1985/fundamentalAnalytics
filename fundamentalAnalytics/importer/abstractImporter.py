'''
Created on Jul 8, 2019

@author: afunes
'''
from abc import abstractmethod
from datetime import datetime
import logging
from pyexpat import ExpatError

from base.dbConnector import DBConnector
from dao.dao import Dao
from dao.fileDataDao import FileDataDao
from tools.tools import createLog, CustomException
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
                self.setFileDataStatus(voList)
                if (voList is None):
                    self.logger.info("***********FINISH AT " + str(datetime.now() - time1) + " " + self.filename)
                else:
                    self.logger.info("***********FINISH AT " + str(datetime.now() - time1) + " " + self.filename + " objects added " + str(len(voList)))
            else:
                self.logger.info("Skipped " + self.filename)
        except (CustomException) as e:
            self.logger.error(self.filename + " " + e.status + " " + str(e))
            self.addOrModifyFDError1(e)
        except MemoryError as e:
            self.logger.error(self.filename + " " + str(e))
            FileDataDao().addOrModifyFileData(statusKey=self.actualStatus, statusValue=Constant.STATUS_ERROR, filename=self.filename, errorMessage='MemoryError', errorKey=self.errorKey)
        except ExpatError as e:
            self.logger.error(self.filename + " " + str(e))
            self.session.rollback()
            self.session.close()
            self.session = DBConnector().getNewSession()
            self.addOrModifyFDError2('not well-formed')
        except Exception as e:
            self.logger.error(self.filename + " " + str(e))
            self.session.rollback()
            self.session.close()
            self.session = DBConnector().getNewSession()
            self.addOrModifyFDError2(str(e)[0:149])
        finally:
            self.session.commit()
            self.session.close()
            
    @abstractmethod
    def setFileDataStatus(self, voList):
        if(voList is not None):
            missingObjects = self.getMissingObjects()
            if(len(voList) == 0):
                setattr(self.fileData, self.actualStatus, Constant.STATUS_NO_DATA)
            elif(len(missingObjects) > 0):
                setattr(self.fileData, self.actualStatus, Constant.STATUS_WARNING)
                FileDataDao().setErrorMessage(errorMessage=str(missingObjects)[0:149], errorKey=self.errorKey, fileData=self.fileData)
            else:    
                setattr(self.fileData, self.actualStatus, Constant.STATUS_OK)
        else: 
            setattr(self.fileData, self.actualStatus, Constant.STATUS_OK) 
        Dao().addObject(objectToAdd=self.fileData, session=self.session, doCommit=True)
    
    @abstractmethod
    def addOrModifyInit(self):
        self.fileDataDao.addOrModifyFileData(statusKey=self.actualStatus, statusValue=Constant.STATUS_INIT, filename=self.filename, errorKey=self.errorKey, externalSession=self.session)
    
    @abstractmethod
    def addOrModifyFDError1(self, e):
        self.fileDataDao.addOrModifyFileData(statusKey=self.actualStatus, statusValue=e.status, filename=self.filename, errorMessage=e.message, 
                                             errorKey=self.errorKey, externalSession=self.session, extraData=e.extraData)
    
    @abstractmethod
    def addOrModifyFDError2(self, errorMessage):
        FileDataDao().addOrModifyFileData(statusKey=self.actualStatus, statusValue=Constant.STATUS_ERROR, filename=self.filename, errorMessage=errorMessage, errorKey=self.errorKey)   
            
    @abstractmethod
    def doImport2(self):
        pass
    
    @abstractmethod
    def getMissingObjects(self):
        return []
    
    @abstractmethod
    def skipOrProcess(self):
        #self.previousStatus is None for fileStatus
        if(self.previousStatus is None or (getattr(self.fileData, self.previousStatus) in [Constant.STATUS_OK, Constant.STATUS_WARNING, Constant.STATUS_NO_DATA])):
            if (getattr(self.fileData, self.actualStatus) != Constant.STATUS_OK or self.replace == True):
                return True
            else:
                self.logger.info("File Data Skipped " + self.filename)
                return False
        else:
            self.logger.info("File Data Skipped " + self.filename)
            return False  
    
    @abstractmethod
    def getPersistent(self, vo):
        pass
    
    @abstractmethod
    def getPersistentList(self, voList):
        if(voList is not None):
            persistentList = []
            for vo in voList:
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
        if (AbstractImporter.logger is None or self.__class__.__name__ != AbstractImporter.logger.name):
            AbstractImporter.logger = createLog(self.__class__.__name__, logging.INFO)
            
    def doCommit(self):
        self.session.commit()
        
    def addOrModifyFDPending(self):
        setattr(self.fileData , self.actualStatus, Constant.STATUS_PENDING)
        Dao().addObject(objectToAdd=self.fileData, session=self.session, doCommit=True)   
