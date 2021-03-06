'''
Created on 9 nov. 2017

@author: afunes
'''
import logging

from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from dataImport.importerExecutor import ImporterExecutor
from engine.importFileEngine import ImportFileEngine
from importer.importerFile import ImporterFile
from modelClass.period import QuarterPeriod
from tools.tools import createLog
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus


if __name__ == "__main__":
    replaceMasterFile = True
    importFDFromSEC = True # True add FD and False process pending FD
    threadNumber = 3
    maxProcessInQueue = 3
    Initializer()
    session = DBConnector().getNewSession()
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    if(importFDFromSEC):
        periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2020, QuarterPeriod.quarter == 2)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        #periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2011)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        #periodList = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2020, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter > 3)), QuarterPeriod.year > 2017)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        for period in periodList:
            ImportFileEngine().importMasterIndexFor(period = period, replaceMasterFile = replaceMasterFile, session = session,threadNumber = threadNumber)
    else: 
        fileDataList = FileDataDao().getFileData6(statusAttr=ConstantStatus.FILE_STATUS, statusValue ='PENDING', session = session)
        #fileDataList = FileDataDao().getFileData6(statusAttr='fileName', statusValue ='edgar/data/1089598/0001014897-19-000042.txt', session = session)
        #fileDataList = FileDataDao().getFileData7(statusAttr=ConstantStatus.FILE_STATUS, statusValue ='ERROR', session = session)
        importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=True, isSequential=True, importerClass=ImporterFile)
        importerExecutor.execute(fileDataList)
