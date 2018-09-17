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

company = GenericDao.getOneResult(Company,Company.ticker.__eq__("TSLA") , session)

for fact in company.factList:
    if (fact.concept.conceptName == "AssetsCurrent"):
        for factValue in fact.factValueList:
            print(factValue.period.startDate, factValue.period.endDate, factValue.period.instant, factValue.value)
         
