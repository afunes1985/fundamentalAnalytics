'''
Created on 9 nov. 2017

@author: afunes
'''
from decimal import Decimal
import httplib

from dao.dao import DaoCompanyFundamental
from modelClass.companyqfundamental import CompanyQFundamental


connection = httplib.HTTPSConnection('api.usfundamentals.com', 443, timeout = 30)
connection.request('GET', '/v1/indicators/xbrl?companies=320193&frequency=q&period_type=yq&token=mQ_RmHg4Dw63ZSK-deZzhQ', None, {})
try:
    response = connection.getresponse()
    content = response.read()
    # Success
    print('Response status ' + str(response.status))
    content = content.strip()
    print(content)
    list1 = content.split("\n")
    headerList = None
    for row in list1:
        if headerList is None:
            headerList = row.split(",")
        else:
            cf = CompanyQFundamental(None)
            for index, item in enumerate(row.split(",")):
                if not isinstance(item, str):
                    item = Decimal(item)
                if item != '':
                    setattr(cf, "q_" + headerList[index], item)
            print(cf.__dict__)
            DaoCompanyFundamental.insertCompanyFundamental(cf)
except httplib.HTTPException:
    print('Exception during request')