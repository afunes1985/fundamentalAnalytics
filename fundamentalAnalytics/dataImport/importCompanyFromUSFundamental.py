'''
Created on 9 nov. 2017

@author: afunes
'''
import httplib
import json

from dao.dao import DaoCompany
from modelClass.company import Company

connection = httplib.HTTPSConnection('api.usfundamentals.com', 443, timeout = 30)
connection.request('GET', '/v1/companies/xbrl?format=json&token=mQ_RmHg4Dw63ZSK-deZzhQ', None, {})
try:
    response = connection.getresponse()
    content = response.read()
    # Success
    print('Response status ' + str(response.status))
    json_data = json.loads(content)
    for x in json_data:
        company = Company(None)
        company.companyID = x['company_id']
        company.name = x['name_latest']
        DaoCompany.insertCompany(company)
except httplib.HTTPException:
    print('Exception during request')