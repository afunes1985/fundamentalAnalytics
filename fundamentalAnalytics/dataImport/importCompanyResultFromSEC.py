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


if __name__ == "__main__":
    COMPANY_TICKER = 'CSCO'
    replace = True
    Initializer()
    session = DBConnector().getNewSession()
    company = GenericDao.getOneResult(Company,Company.ticker.__eq__(COMPANY_TICKER), session)
    #periodList =  session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2018, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter <= 3)), QuarterPeriod.year > 2015)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    periodList =  session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 1)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    createLog('general', logging.DEBUG)
    createLog('bodyIndex', logging.INFO)
    createLog('bodyXML', logging.DEBUG)
    createLog('Error', logging.DEBUG)
    createLog('InvalidOperation_convertToDecimal', logging.DEBUG)
    createLog('notinclude_UnitRef', logging.INFO)
    createLog('notinclude_ContextRef', logging.INFO)
    createLog('skipped_underscore', logging.INFO)
    createLog('skipped', logging.INFO)
    createLog('xsdNotFound', logging.DEBUG)
    createLog('addToDB', logging.INFO)
    createLog('matchList_empty', logging.INFO)
    createLog('tempData', logging.INFO)
    logging.info("START")
    
    for period in periodList:
        fileMasterImporter = FileMasterImporter()
        fileMasterImporter.doImport(period, company, replace, session)
    
