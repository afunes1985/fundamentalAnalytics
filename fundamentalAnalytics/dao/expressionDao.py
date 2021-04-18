'''
Created on Jul 11, 2019

@author: afunes
'''
from base.dbConnector import DBConnector
from dao.dao import GenericDao
from modelClass.expression import Expression
from modelClass.customConcept import CustomConcept


class ExpressionDao(object):

    def getExpressionList(self, session = None):
        return GenericDao().getAllResult(objectClazz = Expression, session = session)
    
    def getExpressionList2(self, isCurrent, session = None):
        if (session is None):
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        objectResult = session.query(Expression)\
            .filter(Expression.isCurrent == isCurrent)\
            .all()
        return objectResult
    
    def getExpressionForReport(self, session = None):
        if (session is None): 
                dbconnector = DBConnector()
                session = dbconnector.getNewSession()
        objectResult = session.query(Expression)\
            .join(Expression.customConcept)\
            .with_entities(CustomConcept.conceptName, Expression.defaultOrder, Expression.periodType, Expression.expression)\
            .all()
        return objectResult
