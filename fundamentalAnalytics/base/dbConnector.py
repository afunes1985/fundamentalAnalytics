'''
Created on 20 ago. 2018

@author: afunes
'''
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker

class DbConnector():
    
    def __init__(self):
        self.engine = create_engine('mysql+mysqlconnector://root:root@localhost/portfolio')
        self.Session = sessionmaker(bind=self.engine)
        self.Session.trust_env = False
        
    def getNewSession(self):
        return self.Session()