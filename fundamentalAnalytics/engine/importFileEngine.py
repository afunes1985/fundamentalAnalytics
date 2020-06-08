'''
Created on May 25, 2019

@author: afunes
'''
from _io import BytesIO, StringIO
from concurrent.futures.thread import ThreadPoolExecutor
import gzip
import logging
import os

import pandas
import requests

from base.dbConnector import DBConnector
from dao.companyDao import CompanyDao
from dao.dao import Dao
from dao.fileDataDao import FileDataDao
from engine.companyEngine import CompanyEngine
from modelClass.fileData import FileData
from tools.tools import getBinaryFileFromCache
from valueobject.constant import Constant


class ImportFileEngine():
    
    def importMasterIndexFor(self, period, replaceMasterFile, session, threadNumber=1):
        file = getBinaryFileFromCache(Constant.CACHE_FOLDER + 'master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                    "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter) + "/master.gz", replaceMasterFile)
        with gzip.open(BytesIO(file), 'rb') as f:
            file_content = f.read()
            text = file_content.decode("ISO-8859-1")
            text = text[text.find("CIK", 0, len(text)): len(text)]
            point1 = text.find("\n")
            point2 = text.find("\n", point1 + 1)
            text2 = text[0:point1] + text[point2 : len(text)]
            df = pandas.read_csv(StringIO(text2), sep="|")
            df.set_index("CIK", inplace=True)
            df.head()
            print("STARTED")
            for row in df.iterrows():
                CIK = row[0]
                filename = row[1]["Filename"]
                formType = row[1]["Form Type"]
                if(formType == "10-Q" or formType == "10-K"):  # edgar/data/1000045/0001564590-19-043374.txt
                    fd = FileDataDao.getFileData(filename, session)
                    if(fd is None):
                        company = CompanyEngine().getOrCreateCompany(CIK=CIK, session=session)
                        fd = FileData()
                        fd.fileName = filename
                        fd.company = company
                        Dao().addObject(objectToAdd=fd, session=session, doCommit=True)
                        print("FD Added " + filename)
            print("FINISHED")
