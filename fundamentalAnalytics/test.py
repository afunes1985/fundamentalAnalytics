'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker

from base.dbConnector import DbConnector
from modelClass.company import Company


dbconnector = DbConnector()
session = dbconnector.getNewSession()

companyList = session.query(Company).all()

for row in companyList:
    print(row.id)