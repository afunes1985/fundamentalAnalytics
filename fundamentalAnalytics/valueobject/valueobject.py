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
        
class FilterFactVO():
    def __init__(self):
        self.CIK = None
        self.conceptName = None
        self.reportShortName = None
        self.ticker = None    
        
class CustomFactValueVO():
    value = None
    origin = None
    fileDataOID = None
    customConcept = None
    endDate = None
    order_ = None
    def __init__(self, value, origin, fileDataOID, customConcept, endDate, order_):
        self.value = value
        self.origin = origin
        self.fileDataOID = fileDataOID
        self.customConcept = customConcept
        self.endDate = endDate
        self.order_ = order_
