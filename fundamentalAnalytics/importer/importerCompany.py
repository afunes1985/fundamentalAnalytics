'''
Created on 19 sep. 2018

@author: afunes
'''

from nt import listdir

import pandas
import xmltodict

from dao.dao import Dao
from engine.companyEngine import CompanyEngine
from importer.abstractFactImporter import AbstractFactImporter
from importer.abstractImporter import AbstractImporter
from tools.tools import getXSDFileFromCache
from valueobject.constant import Constant


class ImporterCompany(AbstractImporter, AbstractFactImporter):

    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_COMPANY, filename, replace, 'fileStatus', 'companyStatus')
        self.processCache = None
            
    def doImport2(self):
        self.processCache = self.initProcessCache2(self.filename, self.session)
        company = self.fillCompanyData(self.session)
        self.fileData.company = company
        Dao().addObject(objectToAdd = self.fileData, session = self.session, doCommit = True) 
    
    def getPersistentList(self, voList):
        return []    
    
    def deleteImportedObject(self):
        pass
       
    def initCache(self):
        pass
#         xsdCache = {}
#         for xsdFileName in listdir(Constant.CACHE_FOLDER + "xsd"):
#             try:
#                 xsdFile = getXSDFileFromCache(Constant.CACHE_FOLDER + "xsd//" + xsdFileName, None)
#                 xsdDict = xmltodict.parse(xsdFile)
#                 xsdDF = pandas.DataFrame(xsdDict["xs:schema"]["xs:element"])
#                 xsdDF.set_index("@id", inplace=True)
#                 xsdDF.head()
#                 xsdCache[xsdFileName] = xsdDF
#                 print(xsdFileName)
#             except Exception as e:
#                 self.logger.exception(e)
#         AbstractImporter.cacheDict["XSD_CACHE"] = xsdCache
    