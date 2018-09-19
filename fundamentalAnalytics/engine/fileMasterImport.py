'''
Created on 19 sep. 2018

@author: afunes
'''
from _io import BytesIO, StringIO
import gzip
import logging

import pandas
from pandas.core.frame import DataFrame

from engine.fileImporter import FileImporter
from tools.tools import getBinaryFileFromCache


class FileMasterImporter():
    def doImport(self, period, company, replace, session):
        logging.getLogger('general').debug("START - Processing index file " + company.ticker  + " " + str(period.year) + "-" +  str(period.quarter) + " " + " replace " + str(replace))   
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
            if (company is not None):
                rowData0 = df.loc[company.CIK]
                if isinstance(rowData0, DataFrame):
                    for rowData1 in rowData0.iterrows():
                        filename = rowData1[1]["Filename"]
                        formType = rowData1[1]["Form Type"]
                        if(formType == "10-Q" or formType == "10-K"):
                            fi = FileImporter(filename, replace)
                            fi.doImport()
                else:
                    filename = rowData0["Filename"]
                    formType = rowData0["Form Type"]
                    if(formType == "10-Q" or formType == "10-K"):
                        FileImporter(filename, replace)
                        fi.doImport()
            else:
                for rowData in df.iterrows():
                    print(rowData)
        logging.getLogger('general').debug("END - Processing index file " + company.ticker  + " " + str(period.year) + "-" +  str(period.quarter) + " " + " replace " + str(replace)) 

