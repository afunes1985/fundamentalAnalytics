'''
Created on 9 nov. 2017

@author: afunes
'''
import logging

from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from engine.importFileEngine import ImportFileEngine
from modelClass.period import QuarterPeriod
from tools.tools import createLog
from valueobject.constant import Constant


if __name__ == "__main__":
    replaceMasterFile = False
    Initializer()
    session = DBConnector().getNewSession()
    periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2019, QuarterPeriod.quarter == 2)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    #periodList = session.query(QuarterPeriod).filter(and_(or_(QuarterPeriod.year < 2020, and_(QuarterPeriod.year >= 2018, QuarterPeriod.quarter > 3)), QuarterPeriod.year > 2017)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    for period in periodList:
        ImportFileEngine.importMasterIndexFor(period, replaceMasterFile, session)