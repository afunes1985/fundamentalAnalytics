'''
Created on 18 ago. 2018

@author: afunes
'''


from base.dbConnector import DBConnector
from dao.dao import Dao, GenericDao
from modelClass.fileData import FileData
from valueobject.constant import Constant


class FactEngine(object):
    @staticmethod
    def deleteFactByFileData(filename, session = None):
        if (session is None): 
            dbconnector = DBConnector()
            session = dbconnector.getNewSession()
        fileData = GenericDao().getOneResult(FileData, FileData.fileName.__eq__(filename), session)
        if(fileData is not None):
            for itemToDelete in fileData.factList:
                session.delete(itemToDelete)
                session.commit()
            fileData.status = Constant.STATUS_PENDING
            fileData.factList = []
            Dao().addObject(objectToAdd = fileData, session = session, doCommit = True)
            print("Object deleted " + str(len(fileData.factList)))