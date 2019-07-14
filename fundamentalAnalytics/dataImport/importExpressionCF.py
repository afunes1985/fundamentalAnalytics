'''
Created on 22 ago. 2018

@author: afunes
'''
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from threading import BoundedSemaphore

from sqlalchemy.sql.expression import and_
from sympy.parsing.sympy_parser import parse_expr

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from dao.expressionDao import ExpressionDao
from importer.importerExpression import ImporterExpression
from modelClass.fileData import FileData
from tools.tools import createLog
from valueobject.constant import Constant


def initMainCache(session):
    expressionDict = {}
    expressionList = ExpressionDao().getExpressionList(session=session)
    for expression in expressionList:
        expr = parse_expr(expression.expression)
        expressionDict[expression] = expr
    return expressionDict

if __name__ == "__main__":
    replace = False
    threadNumber = 1
    conceptName = None
    maxProcessInQueue = 5
    Initializer()
    session = DBConnector().getNewSession()
    createLog(Constant.LOGGER_GENERAL, logging.DEBUG)
    createLog(Constant.LOGGER_ERROR, logging.INFO)
    createLog(Constant.LOGGER_NONEFACTVALUE, logging.INFO)
    createLog(Constant.LOGGER_ADDTODB, logging.INFO)
    logging.info("START")
    fileDataList = GenericDao().getAllResult(FileData, and_(FileData.status.__eq__("OK"), FileData.expressionStatus.__eq__("PENDING")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.importStatus.__eq__("OK"), FileData.status.__eq__("OK"), FileData.entityStatus.__eq__("ERROR")), session)
    #fileDataList = GenericDao().getAllResult(FileData, and_(FileData.fileName == "edgar/data/320193/0000320193-18-000070.txt"), session)
    threads = []    
    expressionDict = initMainCache(session)
    executor = ThreadPoolExecutor(max_workers=threadNumber)
    semaphore = BoundedSemaphore(maxProcessInQueue)
    logging.getLogger(Constant.LOGGER_GENERAL).info("READY TO IMPORT EXPRESSION " + str(len(fileDataList)) + " FILEDATA")
    for fileData in fileDataList:
        try:
            try:
                semaphore.acquire()
                fi = ImporterExpression(fileData.fileName, replace, expressionDict)
                #fi.doImport()
                future = executor.submit(fi.doImport)
            except:
                semaphore.release()
            else:
                future.add_done_callback(lambda x: semaphore.release())
                #pass
        except Exception as e:
                logging.getLogger(Constant.LOGGER_ERROR).exception("ERROR " + fileData.fileName + " " + str(e))
            