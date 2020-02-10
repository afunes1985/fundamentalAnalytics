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
            company.listed = True
        elif(company.ticker is None and ticker is not None):
            company.ticker = ticker
            company.entityRegistrantName = entityRegistrantName
        company.entityRegistrantName = entityRegistrantName
        Dao().addObject(objectToAdd = company, session = session, doCommit = True)
        return company
    
    def updateListedCompany(self, company, noTradingSymbolFlag, session):
        if(noTradingSymbolFlag is not None and noTradingSymbolFlag):
            company.listed = False
            company.notListedDescription = 'noTradingSymbolFlag'
        Dao().addObject(objectToAdd = company, session = session, doCommit = True)
        
    def getCompanyWithJustFilename(self, filename, session):
        CIK = filename[len("edgar/data/"):filename.rfind("/",0, len(filename))]
        company = CompanyEngine().getOrCreateCompany(CIK, None, None, session)
        return company
        