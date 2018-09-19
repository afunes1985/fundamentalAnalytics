'''
Created on 9 nov. 2017

@author: afunes
'''
from decimal import Decimal
import logging

import requests

from base.dbConnector import DbConnector
from modelClass.company import Company
from modelClass.companyqresult import CompanyQResult


dbconnector = DbConnector()
session = dbconnector.getNewSession()

ticker = 'INTC'

company = session.query(Company)\
.filter(Company.ticker.__eq__(ticker))\
.one()

response = requests.get("https://api.usfundamentals.com/v1/indicators/xbrl?companies=" + company.companyID + "&frequency=q&period_type=yq&token=mQ_RmHg4Dw63ZSK-deZzhQ", timeout = 10) 
print('Response status ' + str(response.status_code))

try:
    content = response.text
    content = content.strip()
    list1 = content.split("\n")
    headerList = None
    print(list1)
    for row in list1:
        if headerList is None:
            headerList = row.split(",")
        else:
            for index, item in enumerate(row.split(",")):
                if(headerList[index] == "company_id"):
                    companyID = item
                elif(headerList[index] == "indicator_id"):
                    indicatorID = item
                else:
                    cqr = CompanyQResult()
                    cqr.companyID = companyID
                    cqr.indicatorID = indicatorID
                    cqr.year = headerList[index][0:4]
                    cqr.quarter = headerList[index][5:6]
                    if item.isdigit() or item[0:1] == "-":
                        cqr.value = Decimal(item)
                    session.add(cqr)
                    print(cqr.__dict__)
    session.commit()
except Exception as e:
    logging.warning(e)