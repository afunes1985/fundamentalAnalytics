'''
Created on 9 nov. 2017

@author: afunes
'''
import httplib
import json
import logging

from base.dbConnector import DbConnector
from modelClass.company import Company


try:
    connection = httplib.HTTPSConnection('api.iextrading.com', 443, timeout = 30)
    
    dbconnector = DbConnector()
    session = dbconnector.getNewSession()
    
    companyList = session.query(Company)\
            .filter(Company.ticker.isnot(None))\
            .all()
    
    for company in companyList:
        url = "/1.0/stock/" + company.ticker + "/company"
        print(url)
        connection.request('GET', url, None, {})
        response = connection.getresponse()
        content = response.read()
        print('Response status ' + str(response.status))
        if (response.status == 200):
            json_data = json.loads(content)
            print(json_data)
            print(json_data["sector"])
            company.sector = json_data["sector"]
    session.commit()
except Exception as e:
    print(e)