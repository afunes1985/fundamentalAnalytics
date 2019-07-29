'''
Created on Jul 6, 2019

@author: afunes
'''
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_

from base.dbConnector import DBConnector
from modelClass.company import Company
from modelClass.fact import Fact
from modelClass.factValue import FactValue
from modelClass.fileData import FileData
from modelClass.period import Period

class PeriodDao():
    pass
