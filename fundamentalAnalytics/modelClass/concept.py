'''
Created on 7 ago. 2018

@author: afunes
'''
class Concept():
    def __init__(self, row):
        if(row is not None):
            self.setAttr(row[0], row[1], row[2], row[3])
    
    def setAttr(self, OID, section, indicatorID, label):
        self.OID = OID
        self.section = section
        self.indicatorID = indicatorID
        self.label = label