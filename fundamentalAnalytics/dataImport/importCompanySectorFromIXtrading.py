'''
Created on 9 nov. 2017

@author: afunes
'''
from http import client
import json
import logging

import requests

from base.dbConnector import DbConnector
from modelClass.company import Company


try:
    #connection = client.HTTPSConnection('api.iextrading.com', 443, timeout = 30)
    
    dbconnector = DbConnector()
    session = dbconnector.getNewSession()
    
    companyList = session.query(Company)\
            .filter(Company.ticker.isnot(None))\
            .all()
    
    for company in companyList:
        try:
            url = "https://api.iextrading.com/1.0/stock/" + company.ticker + "/company"
            print(url)
            response = requests.get(url, timeout = 10) 
            print('Response status ' + str(response.status_code))
            if (response.status_code == 200):
                json_data = response.json()
                print(json_data)
                print(json_data["sector"])
                company.sector = json_data["sector"]
                session.commit()
        except Exception as e:
            print(e)       
except Exception as e:
    print(e)