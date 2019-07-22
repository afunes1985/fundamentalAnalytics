'''
Created on Jul 11, 2019

@author: afunes
'''
from dao.dao import GenericDao
from modelClass.expression import Expression


class ExpressionDao(object):

    def getExpressionList(self, session = None):
        return GenericDao().getAllResult(objectClazz = Expression, session = session)
