'''
Created on 9 nov. 2017

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import Dao, GenericDao
from modelClass.company import Company
from dao.companyDao import CompanyDao
from modelClass.ticker import Ticker


Initializer()
session = DBConnector().getNewSession()

#companyList = GenericDao().getAllResult(Company, (Company.ticker.isnot(None)), session)
notMarchingList = []
fileHandle = open('C:\\Users\\afunes\\iCloudDrive\\PortfolioViewer\\import\\ticker.txt', 'r')
objectList = []
for line in fileHandle:
    row = line.split('\t')
    ticker = row[0].upper()
    CIK = row[1].rstrip()
    company = CompanyDao().getCompany2(CIK, session)
    if (company is None):
        print("Company not found CIK: " + CIK)
    elif (company is not None):
        tickerList = [x.ticker for x in company.tickerList]
        if (ticker not in tickerList):
            t = Ticker()
            t.ticker = ticker.upper()
            t.tickerOrigin = "www.sec.gov/include/ticker.txt" 
            company.tickerList.append(t)
            print("Ticker added " + ticker)
            Dao().addObject(objectToAdd = company, session = session, doFlush = True)
session.commit()
# fileHandle = open('C:\\Users\\afunes\\iCloudDrive\\PortfolioViewer\\import\\nasdaqlisted.txt', 'r')
# for line in fileHandle:
#     row = line.split('|')
#     if ("Common Stock".upper() in row[1].upper()):
#         if (row[6] != 'Y'):
#             #print(row)
#             exists = False
#             for company in companyList:
#                 if (company.ticker == row[0]):
#                     exists = True
#                     break
#             if not exists:
#                 notMarchingList.append(row[0] + "  " + row[2])
#  
# fileHandle.close()
#          
# for item in notMarchingList:
#     print(item)

# 
# for company in companyList:
#     if(company.ticker is None
#        and company.CIK is not None):
#         try:
#             company.ticker = object_[company.CIK]
#             session.add(company)
#         except Exception as e:
#             print(repr(e))
# session.commit()
# # 

# import csv
# with open('C:\\Users\\afunes\\iCloudDrive\\PortfolioViewer\\import\\nyse-listed_csv.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader: