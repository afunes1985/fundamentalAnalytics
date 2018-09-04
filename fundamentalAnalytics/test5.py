'''
Created on 26 ago. 2018

@author: afunes
'''
from pandas.core.frame import DataFrame

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao, DaoCompanyResult
from modelClass.company import Company
from modelClass.concept import Concept


Initializer()
session = DBConnector().getNewSession()

BALANCE = "Balance sheet"
CASH_FLOW = "Cash flow statement"
INCOME = "Income statement"
INCOME_COMPREHENSIVE = "Comprehensive income"
GENERAL = "General"
KEY_RATIOS = "Key Ratios"

rs = DaoCompanyResult.getCompanyResult(ticker="TSLA", sectionID = BALANCE);

rows = rs.fetchall()

for row in rows:
    print(row[0] + ";" + row[1] + ";" + row[2] + ";" + str(row[3]) + ";" + str(row[4]) + ";" + str(row[5]))

if (len(rows) != 0):
    df = DataFrame(rows)
    df.columns = rs.keys()

#print(df.to_string())