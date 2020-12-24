import logging

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dataImport.importerExecutor import ImporterExecutor
from importer.importerPrice import ImporterPrice
from dataImport.importerMassiveExecutor import ImporterMassiveExecutor

if __name__ == "__main__":
    Initializer()
    session = DBConnector().getNewSession()
    ImporterMassiveExecutor().execute(session)
