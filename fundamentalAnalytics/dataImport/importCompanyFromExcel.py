import logging
import math

import pandas

from base.dbConnector import DbConnector
from modelClass.company import Company


df = pandas.read_csv('C://Users//afunes//iCloudDrive//PortfolioViewer//import//companylist-amex.csv');
df.fillna("")
#get the values for a given column
symbolValues = df['Symbol'].values
sectorValues = df['Sector'].values
industryValues = df['industry'].values

try:
    dbconnector = DbConnector()
    session = dbconnector.getNewSession()
    count = 0
    for index, symbolValue in enumerate(symbolValues):
        if(sectorValues[index] != ""
            and industryValues[index] != ""):
            try:
                session.query(Company).filter(Company.ticker == symbolValue).\
                    update({Company.sector: sectorValues[index], Company.industry: industryValues[index]}, synchronize_session=False)
                count = count + 1
            except Exception as e:
                logging.warning(e) 
    session.commit()
    print (count)
except Exception as e:
    print(e) 