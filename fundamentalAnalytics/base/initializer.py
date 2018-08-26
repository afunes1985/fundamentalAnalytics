'''
Created on 26 ago. 2018

@author: afunes
'''
from pyramid.config import Configurator

from base.dbConnector import DBConnector
from modelClass import initialize_sql


class Initializer():
    def __init__(self):
        dbConnector = DBConnector() 
        config = Configurator()
        config.scan('modelClass') 
        initialize_sql(dbConnector.engine)
        config.make_wsgi_app()