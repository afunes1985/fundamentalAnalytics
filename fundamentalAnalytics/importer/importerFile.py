'''
Created on May 25, 2019

@author: afunes
'''
import gzip
import os

import requests

from importer.abstractImporter import AbstractImporter
from tools.tools import XMLNotFoundException, getXMLDictFromGZCache, \
    getXMLFromText
from valueobject.constant import Constant


class ImporterFile(AbstractImporter):
    
    def __init__(self, filename, replace):
        AbstractImporter.__init__(self, Constant.ERROR_KEY_FILE, filename, replace, None, 'fileStatus')
    
    def doImport2(self):
        fullFileName = Constant.CACHE_FOLDER + self.filename
        fullFileName = fullFileName[0: fullFileName.find(".txt")]
        if(self.validateIfSomeFilesNotExits(fullFileName) or self.replace):
            url = "https://www.sec.gov/Archives/" +self.filename
            response = requests.get(url, timeout = 30) 
            fileText = response.text
            if not os.path.exists(fullFileName): 
                os.makedirs(fullFileName)
            self.saveFile2(fileText,"FILENAME", Constant.DOCUMENT_SUMMARY, ["XML", "XBRL"], fullFileName)
            try:
                self.saveFile(fileText,"TYPE", Constant.DOCUMENT_INS, "XBRL",fullFileName)
            except XMLNotFoundException:#TODO mejorar esto
                summaryDict = getXMLDictFromGZCache(self.filename, Constant.DOCUMENT_SUMMARY)
                for file in summaryDict["FilingSummary"]["InputFiles"]['File']:
                    if isinstance(file, dict):
                        if(file["@doctype"] == "10-Q") or file["@doctype"] =="10-K":
                            instFilename = file["#text"]
                            instFilename = instFilename.replace(".", "_") + ".xml"
                            self.saveFile(fileText,"FILENAME", instFilename, "XML",fullFileName, hardKey=Constant.DOCUMENT_INS)
                            break
            self.saveFile(fileText,"TYPE", Constant.DOCUMENT_SCH, "XBRL",fullFileName, True)
            self.saveFile(fileText,"TYPE", Constant.DOCUMENT_PRE, "XBRL",fullFileName) 
    
    def validateIfSomeFilesNotExits(self, folder):
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_INS + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_PRE + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SCH + ".gz"):
            return True
        if not os.path.exists(folder + "//" + Constant.DOCUMENT_SUMMARY + ".gz"):
            return True
    
    def saveFile(self, fileText, tagKey, key, mainTag, fullFileName, skipIfNotExists = False, hardKey = None):
        if(hardKey is None):
            hardKey = key
        xmlFile= getXMLFromText(fileText, tagKey, key, mainTag, skipIfNotExists)
        if(xmlFile is not None):
            with gzip.open(fullFileName + "//" + hardKey + ".gz", 'wb') as f:
                f.write(xmlFile.encode())
    
    def saveFile2(self, fileText, tagKey, key, mainTagList,fullFileName):
        for mainTag in mainTagList:
            try:
                self.saveFile(fileText, tagKey, key, mainTag, fullFileName)
                break
            except Exception as e:
                if(mainTagList[-1] == mainTag):
                    raise e
                else:
                    print(e)