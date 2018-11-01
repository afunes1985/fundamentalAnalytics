'''
Created on 8 sep. 2018

@author: afunes
'''
class ContextRef():
    def __init__(self):
        self.context = None
        self.starDate = None
        self.endDate = None

class FactValueVO():
    def __init__(self):
        self.contextRef = None
        self.unitRef = None
        self.value = None
    
class FactVO():
    def __init__(self):
        self.xlink_href = None
        self.conceptName = None
        self.factValueList = []
        self.reportRole = None
        self.order = None
    
    def getXsdURL(self):
        return self.xlink_href[0: self.xlink_href.find("#", 0)]
    
    def getConceptID(self):
        return self.xlink_href[self.xlink_href.find("#", 0) + 1:len(self.xlink_href)]
    
class CompanyVO():
    def __init__(self):
        self.CIK = None
        