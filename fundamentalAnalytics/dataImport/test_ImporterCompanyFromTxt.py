'''
Created on 9 nov. 2017

@author: afunes
'''
import requests

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.companyDao import CompanyDao
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from modelClass.ticker import Ticker


if __name__ == "__main__":
    Initializer()
    notMarchingList = []
#     fileHandle = open('C:\\Users\\afunes\\iCloudDrive\\PortfolioViewer\\import\\ticker.txt', 'r')
    url = "https://www.sec.gov/include/ticker.txt"
    response = requests.get(url, timeout = 30) 
    fileText = response.text
    objectList = []
    for line in fileText.split('\n'):
        session = DBConnector(isNullPool=True).getNewSession()
        row = line.split('\t')
        ticker = row[0].upper()
        CIK = row[1].rstrip()
#         print(ticker, CIK)
        company = CompanyDao().getCompany2(CIK, session)
        if (company is None):
            print("Company not found CIK: " + CIK)
        elif (company is not None):
            tickerList = [x.ticker for x in company.tickerList]
            if (ticker not in tickerList):
                t = Ticker()
                t.ticker = ticker.upper()
                t.tickerOrigin = "www.sec.gov/include/ticker.txt" 
                t.active = True
                company.tickerList.append(t)
                print("Adding ticker " + ticker + " for company " + str(company.CIK))
                try:
                    Dao().addObject(objectToAdd = company, session = session, doFlush = True)
                    session.commit()
                except Exception as e:
                    print(str(e))
        
