'''
Created on Jul 7, 2019

@author: afunes
'''
from dao.companyDao import CompanyDao
from dao.dao import Dao
from modelClass.company import Company


class CompanyEngine():
    
    def getOrCreateCompany(self, CIK, ticker, entityRegistrantName, session):
        company = CompanyDao().getCompany2(CIK, session)
        if(company is None):    
            company = Company()
            company.CIK = CIK
            company.entityRegistrantName = entityRegistrantName
            company.ticker = ticker
            Dao().addObject(objectToAdd = self.company, session = session, doCommit = True)
        if(company.ticker is None and ticker is not None):
            company.ticker = ticker
            Dao().addObject(objectToAdd = self.company, session = session, doCommit = True)
        return company