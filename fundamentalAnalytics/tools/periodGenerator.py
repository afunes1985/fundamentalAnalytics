'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from modelClass.period import Period


Initializer()
session = DBConnector().getNewSession()

for x in range(1, 9):
    year = "201" + str(x)
    year = int(year)
    for y in range(1, 5):
        period1 = Period()
        period1.year = year
        period1.quarter = y
        session.add(period1)
    session.commit()