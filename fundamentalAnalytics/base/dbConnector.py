'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker

class DBConnector():
    
    def __init__(self):
        self.engine = create_engine('mysql+mysqlconnector://root:root@localhost/fundamenalanalytics')
        self.Session = sessionmaker(bind=self.engine, autoflush=False)
        self.Session.trust_env = False
        
    def getNewSession(self):
        return self.Session()