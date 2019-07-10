'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool.impl import QueuePool, SingletonThreadPool, NullPool


class DBConnector(object):
    __instance = None
    session = None
    engine = None
    
    def __init__(self):
        self.engine = create_engine('mysql+mysqlconnector://root:root@localhost/fundamentalanalytics', poolclass=NullPool)
        self.session = sessionmaker(bind=self.engine, autoflush=False)
        self.session.trust_env = False
    
#     def __new__(cls):
#         if DBConnector.__instance is None:
#             DBConnector.__instance = object.__new__(cls)
#         return DBConnector.__instance
        
    def getNewSession(self):
        return self.session()