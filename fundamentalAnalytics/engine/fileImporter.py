'''
Created on 19 sep. 2018

@author: afunes
'''
import logging

from base.dbConnector import DBConnector
from dao.dao import Dao
from engine.abstractFileImporter import AbstractFileImporter


class FileImporter(AbstractFileImporter):
    # def getTagValue(xmlDict, elementID, attrID, periodDict):
    #     element = xmlDict[elementID]
    #     if isinstance(element, list):
    #         for ele in element:
    #             if (periodDict.get(element["@contextRef"], -1) != -1):
    #def isPeriodAllowed():
     #   periodDict = processCache[Constant.PERIOD_DICT]
    def __init__(self, filename, replace):
        self.session = DBConnector().getNewSession()
        self.processCache = self.initProcessCache(filename, self.session)
        self.filename = filename
            
    def doImport(self):
        logging.getLogger('general').debug("START - Processing filename " + self.filename)
        fileData = self.getFileData(self.processCache, self.filename, self.session)
        reportDict = self.getReportDict(self.processCache, self.session)
        factToAddList = self.getFactByReport(reportDict, self.processCache, self.session)
        factToAddList = self.setFactValues(factToAddList, self.processCache)
        Dao.addFact(factToAddList, self.company, fileData, self.session)
        #for factVO in factToAddList:
            #logging.getLogger('general').debug(factVO.report.shortName + " " + factVO.conceptName + " " + str(factVO.factValueList)) 
        logging.getLogger('general').debug("END - Processing filename " + self.filename)
        

