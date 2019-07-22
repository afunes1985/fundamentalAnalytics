'''
Created on Jul 22, 2019

@author: afunes
'''
from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from modelClass.price import Price


class PriceDao(object):

    def deletePriceByFD(self, fileDataOID, session=None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        session.query(Price).filter(and_(Price.fileDataOID == fileDataOID)).delete()
