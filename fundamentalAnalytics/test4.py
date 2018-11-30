'''
Created on 26 ago. 2018

@author: afunes
'''

from datetime import datetime

import pandas
from pandas.core.frame import DataFrame

from dao.dao import DaoCompanyResult


ticker = 'INTC'
conceptName3 = 'CashAndCashEquivalentsAtCarryingValue'
conceptName3 = None
time1 = datetime.now()
rs = DaoCompanyResult.getFactValues2(ticker = ticker, conceptName = conceptName3, periodType = None)
print("STEP 1 " + str(datetime.now() - time1))
rows = rs.fetchall()
# conceptName2 = None
# newRowsDict = {}
# for row in rows:
#     if(conceptName2 is None
#        or conceptName2 != row[1]):
#         conceptName2 = row[1]
#         newRowsDict[conceptName2] = [row[3]]
#     else:
#         newRowsDict[conceptName2] = newRowsDict[conceptName2] + [row[3]]
# print(newRowsDict)
# 
# s = pandas.Series(newRowsDict)
# df = s.to_frame()
# print(DataFrame(df).to_string())
# 
df = DataFrame()
conceptName = None
for row in rows:
    if(conceptName is None
       or conceptName != row[1]):
        conceptName = row[1]
        if(df.get("reportName", None) is not None):
            print(df["reportName"])
            df["reportName"] = df["reportName"] + [row[0]]
        else:
            df["reportName"] = [row[0]]
             
        if(df.get("conceptName", None) is not None):
            df["conceptName"] = df["conceptName"] + [conceptName]
        else:
            df["conceptName"] = [conceptName]
    if(df.get(row[4], None) is not None):
        df[row[4]] = df[row[4]] + [row[3]]
    else:
        df[row[4]] = [row[3]]
    #columnList.append(row[2])
    #dataList.append(row[1])
print(df.to_string())   

    
#     
#     
# if (len(rows) != 0):
#     df = DataFrame(rows)
#     df.columns = rs.keys()
#     df2 = DataFrame(df["value"])
#     df2.columns = df["date_"]
#     print(df2.to_string())