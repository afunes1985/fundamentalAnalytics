'''
Created on 22 ago. 2018

@author: afunes
'''
import logging

from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from engine.fileMasterImport import FileMasterImporter
from modelClass.company import Company
from modelClass.period import QuarterPeriod
from tools.tools import createLog
from valueobject.constant import Constant


if __name__ == "__main__":
    COMPANY_TICKER = None
    replace = True
    Initializer()
    session = DBConnector().getNewSession()
    if (COMPANY_TICKER is not None):
        company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
    else:
        company = None
    #periodList =  session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2018, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter <= 3)), QuarterPeriod.year > 2015)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    periodList =  session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 1)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    createLog(Constant.LOGGER_GENERAL, logging.DEBUG)
    createLog(Constant.LOGGER_ERROR, logging.DEBUG)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.DEBUG)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    
    for period in periodList:
        fileMasterImporter = FileMasterImporter()
        fileMasterImporter.doImport(period, company, replace, session)
    
