'''
Created on 9 nov. 2017

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging

from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.importFileEngine import ImportFileEngine
from modelClass.fileData import FileData
from modelClass.period import QuarterPeriod
from tools.tools import createLog
from valueobject.constant import Constant
from valueobject.valueobject import ImportFileVO


if __name__ == "__main__":
    replaceMasterFile = False
    useQuarterPeriod = True
    threadNumber = 5
    Initializer()
    session = DBConnector().getNewSession()
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    if(useQuarterPeriod):
        periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year >= 2012, QuarterPeriod.quarter == 4)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        #periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2011)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        #periodList = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2020, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter > 3)), QuarterPeriod.year > 2017)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        for period in periodList:
            ImportFileEngine.importMasterIndexFor(period, replaceMasterFile, session,threadNumber = threadNumber)
    else:
        fileDataList = GenericDao.getAllResult(FileData, and_(FileData.importStatus == "IMP FNF"), session)
        executor = ThreadPoolExecutor(max_workers=threadNumber)
        print("START")
        for filedata in fileDataList:
            importVO = ImportFileVO(filedata.fileName)
            executor.submit(importVO.importFile)
        print("FINISHED")