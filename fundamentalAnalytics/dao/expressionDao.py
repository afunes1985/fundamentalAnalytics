'''
Created on Jul 11, 2019

@author: afunes
'''
from dao.dao import GenericDao
from modelClass.expression import Expression


class ExpressionDao(object):

    def getExpression(self, expressionName, session = None):
        return GenericDao().getOneResult(objectClazz = Expression, condition = Expression.name == expressionName, session = session, raiseNoResultFound=False)
    
    def getExpressionList(self, session = None):
        return GenericDao().getAllResult(objectClazz = Expression, session = session)