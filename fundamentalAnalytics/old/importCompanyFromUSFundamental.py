'''
Created on 9 nov. 2017

@author: afunes
'''
import logging

import requests

from modelClass.company import Company

response = requests.get("https://api.usfundamentals.com/v1/companies/xbrl?format=json&token=mQ_RmHg4Dw63ZSK-deZzhQ", timeout = 10) 
print('Response status ' + str(response.status_code))
try:
    json_data = response.json()
    for x in json_data:
        print(x['company_id'])
        #company = Company()
        #company.companyID = x['company_id']
        #company.name = x['name_latest']
except Exception as e:
    logging.warning(e)
    
    
    