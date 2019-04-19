'''
Created on 18 ago. 2018

@author: afunes
'''

from sympy.parsing.sympy_parser import parse_expr

from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.customFactValue import CustomFactValue
from modelClass.fileData import FileData


class FactEngine(object):
    @staticmethod
    def deleteFactByFileData(filename, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        fileData = GenericDao.getOneResult(FileData, FileData.fileName.__eq__(filename), session)
        if(fileData is not None):
            for itemToDelete in fileData.factList:
                session.delete(itemToDelete)
                session.commit()
            fileData.status = 'PENDING'
            fileData.factList = []
            Dao.addObject(objectToAdd = fileData, session = session, doCommit = True)
            print("Object deleted " + str(len(fileData.factList)))