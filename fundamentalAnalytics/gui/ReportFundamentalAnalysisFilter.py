'''
Created on 12 ago. 2018

@author: afunes
'''
from PySide import QtGui
from PySide.QtGui import QPushButton, QSizePolicy

from frame.framework import PanelWithTable


class ReportFundamentalAnalysisFilter(PanelWithTable):
    
    def __init__(self, parent): 
        super(ReportFundamentalAnalysisFilter, self).__init__()
        self.parent = parent
        self.layout = QtGui.QGridLayout(self)
        #btnSubmit
        self.btnSubmit = QPushButton("Submit", self)
        self.btnSubmit.setSizePolicy(QSizePolicy.Fixed,QSizePolicy.Fixed)
        self.layout.addWidget(self.btnSubmit)
        self.setFixedSize(190, 150) 
        self.initListener() 
        
    def initListener(self):
        self.btnSubmit.clicked.connect(self.doSubmit)
    
    def doSubmit(self):
        self.parent.doSubmit(None)