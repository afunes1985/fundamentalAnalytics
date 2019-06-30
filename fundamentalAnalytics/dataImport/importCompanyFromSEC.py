'''
Created on 9 nov. 2017

@author: afunes
'''
from _io import BytesIO, StringIO
import gzip
import logging

import pandas
from pandas.core.frame import DataFrame
from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
from modelClass.company import Company
from modelClass.period import QuarterPeriod
from tools.tools import getBinaryFileFromCache, getTxtFileFromCache, \
    getXmlDictFromText, createLog
from valueobject.constant import Constant


def readSECIndexFor(period, replace, session):
    file = getBinaryFileFromCache('C://Users//afunes//iCloudDrive//PortfolioViewer//cache//master' + str(period.year) + "-Q" + str(period.quarter) + '.gz',
                                "https://www.sec.gov/Archives/edgar/full-index/" + str(period.year) + "/QTR" + str(period.quarter)+ "/master.gz")
    with gzip.open(BytesIO(file), 'rb') as f:
        file_content = f.read()
        text = file_content.decode("ISO-8859-1")
        text = text[text.find("CIK", 0, len(text)): len(text)]
        point1 = text.find("\n")
        point2 = text.find("\n", point1+1)
        text2 = text[0:point1] + text[point2 : len(text)]
        df = pandas.read_csv(StringIO(text2), sep="|")
        df.set_index("CIK", inplace=True)
        df.head()
        count = 0
        for row in df.iterrows():
            count += 1
            filename = row[1]["Filename"]
            formType = row[1]["Form Type"]
            if(formType == "10-Q" or formType == "10-K"):
                try:
                    fileText = getTxtFileFromCache("C://Users//afunes//iCloudDrive//PortfolioViewer//cache//" + filename, 
                                        "https://www.sec.gov/Archives/" + filename)
                    #print(filename)
                    processCache = {}
                    insXMLDict = getXmlDictFromText(fileText,"TYPE",Constant.DOCUMENT_INS,"XBRL")
                    processCache[Constant.DOCUMENT_INS] = insXMLDict
                    fileData = FileDataDao.getFileData(processCache, filename, session)
                    print(fileData.__dict__)
                    company = Company()
                    company.entityCentralIndexKey = fileData.entityCentralIndexKey
                    company.entityRegistrantName = fileData.entityRegistrantName
                    company.ticker = fileData.tradingSymbol.upper()
                    session.add(company)
                    if(count % 10 == 0):
                        session.commit()
                except Exception as e:
                    print(e)
        session.commit()

Initializer()
session = DBConnector().getNewSession()
periodList = session.query(QuarterPeriod).filter(and_(QuarterPeriod.year == 2018, QuarterPeriod.quarter == 1)).order_by(QuarterPeriod.year.asc(), QuarterPeriod.quarter.asc()).all()
logging.info("START")
createLog('general', logging.DEBUG)
for period in periodList:
    readSECIndexFor(period, False, session)