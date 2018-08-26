'''
Created on 26 ago. 2018

@author: afunes
'''
from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import GenericDao
from modelClass.company import Company


Initializer()
session = DBConnector().getNewSession()

company = GenericDao.getOneResult(Company,Company.ticker.__eq__("INTC") , session)

for cqr in company.companyQResultList:
    print(cqr.period.year, cqr.period.quarter, cqr.concept.conceptID,  cqr.value)