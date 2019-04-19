'''
Created on 22 ago. 2018

@author: afunes
'''
import logging
from nt import listdir
from threading import Semaphore
import threading

import pandas
from sqlalchemy.sql.expression import and_, or_
import xmltodict

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.fileImporter import FileImporter
from engine.fileMasterImport import FileMasterImporter
from modelClass.company import Company
from modelClass.fileData import FileData
from modelClass.period import QuarterPeriod
from tools.tools import createLog, getXSDFileFromCache
from valueobject.constant import Constant


def initMainCache():
    mainCache = {}
    for xsdFileName in listdir(Constant.CACHE_FOLDER + "xsd"):
        try:
            xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, None)
            xsdDict = xmltodict.parse(xsdFile)
            xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
            xsdDF.set_index("@id", inplace=True)
            xsdDF.head()
            mainCache[xsdFileName] = xsdDF
            print(xsdFileName)
        except Exception:
            pass
    return mainCache

if __name__ == "__main__":
    COMPANY_TICKER = None
    replace = False
    userMasterIndex = False
    Initializer()
    session = DBConnector().getNewSession()
    if (COMPANY_TICKER is not None):
        company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
    else:
        company = None
    
    createLog(Constant.LOGGER_GENERAL, logging.INFO)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    
    if(userMasterIndex):#NO FUNCIONAAAAAAAAAAAAAAAAAA
        #periodList =  session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2018, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter <= 3)), QuarterPeriod.year > 2015)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        periodList =  session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 4)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
        for period in periodList:
            fileMasterImporter = FileMasterImporter()
            fileMasterImporter.doImport(period, company, replace, session)
    else:    
        fileDataList = GenericDao.getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("PENDING")), session)
        #fileDataList = GenericDao.getAllResult(FileData, and_(FileData.fileName == "edgar/data/1016708/0001477932-18-002398.txt"), session)
        threads = []    
        s = Semaphore(1)
        mainCache = initMainCache()
        for fileData in fileDataList:
            try:
                s.acquire()
                fi = FileImporter(fileData.fileName, replace, mainCache,  s)
                #fi.doImport(replace)
                t = threading.Thread(target=fi.doImport)
                t.start()
                threads.append(t)
            except Exception as e:
                    logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
        
        for thread in threads:
            thread.join()
            
            