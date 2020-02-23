from base.dbConnector import DBConnector
from dao.dao import GenericDao, Dao
from dao.entityFactDao import EntityFactDao
from modelClass.fileData import FileData
from valueobject.constant import Constant


class EntityFactEngine(object):

#     def deleteCustomFactByCompany(self, ticker, session):
#         entityFactValueList = EntityFactDao().getEntityFactList2(ticker=ticker, session=session);
#         if(len(entityFactValueList) > 0): 
#             for itemToDelete in entityFactValueList:
#                 session.delete(itemToDelete)
#             session.commit()
#             print("DELETED " + str(len(entityFactValueList)))