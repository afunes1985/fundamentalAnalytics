'''
Created on Apr 19, 2019

@author: afunes
'''
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_, text

from base.dbConnector import DBConnector
from dao.dao import GenericDao
from modelClass.company import Company


class CompanyDao():
    
    def getCompany(self, ticker, session):
        return GenericDao.getOneResult(Company, and_(Company.ticker.__eq__(ticker)), session, raiseNoResultFound = False)

    def getCompany2(self, CIK, session):
        return GenericDao.getOneResult(Company,Company.CIK.__eq__(CIK), session, raiseNoResultFound = False)

    def getCompanyList(self, session = None):
        dbconnector = DBConnector()
        with dbconnector.engine.connect() as con:
            query = text("""select distinct company.CIK, company.entityRegistrantName, company.ticker
                                FROM fa_company company
                                    join fa_file_data fd on fd.companyOID = company.OID
                                order by company.entityRegistrantName""")
            rs = con.execute(query, [])
            return rs 