'''
Created on 9 nov. 2017

@author: afunes
'''
import httplib
import json
import logging

from dao.dao import DaoCompany
from modelClass.company import Company


connection = httplib.HTTPSConnection('api.iextrading.com', 443, timeout = 30)
connection.request('GET', '/1.0/stock/market/collection/sector?collectionName=Health%20Care', None, {})
try:
    response = connection.getresponse()
    content = response.read()
    # Success
    print('Response status ' + str(response.status))
    json_data = json.loads(content)
    for x in json_data:
        
#         print (x['companyName'])
#         companyName = x['companyName']
#         rs = DaoCompany.getCompanyByName(companyName[0:len(companyName)-1])
#         if (len(rs)== 1):
#             print("OK")
#         else:
#             rs = DaoCompany.getCompanyByName(companyName)
#             if (len(rs)== 1):
#                 print("OK")
#             else:
#                 logging.warning("NOT OK")

        print(x)
except httplib.HTTPException:
    print('Exception during request')