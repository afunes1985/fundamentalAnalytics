'''
Created on 12 ago. 2018

@author: afunes
'''
from PySide import QtGui, QtCore
from PySide.QtGui import QTableWidget
from dao.dao import DaoCompanyFundamental
from frame.ReportFundamentalAnalysisFilter import ReportFundamentalAnalysisFilter
from frame.framework import PanelWithTable
from modelClass.constant import Constant


class ReportFundamentalAnalysis(PanelWithTable):
    
    columnList = [];
    
    def __init__(self): 
        super(self.__class__, self).__init__()
        self.layout = QtGui.QGridLayout(self)
        self.reportFundamentalAnalysisFilter = ReportFundamentalAnalysisFilter(self)
        self.layout.addWidget(self.reportFundamentalAnalysisFilter, 1, 0, QtCore.Qt.AlignTop)
        self.layout.addWidget(self.createTable(), 1, 1, QtCore.Qt.AlignTop)
        
    def createTable(self):
        self.table = QTableWidget()
        self.table.setRowCount(1000)
        self.table.setColumnCount(len(self.columnList))
        self.table.setEditTriggers(QtGui.QAbstractItemView.NoEditTriggers)
        self.table.setHorizontalHeaderLabels(self.columnList)
        #self.pnLTableWidget.resizeColumnsToContents()
        #self.table.sortItems(Constant.CONST_COLUMN_FUNDAMENTAL_ANALYSIS_INDICATOR_ID)
        self.table.resizeRowsToContents()
        self.table.setFixedSize(1100, 900) 
        return self.table 
    
    def doSubmit(self, companyID):
        resultDict = DaoCompanyFundamental.getCompanyFundamental2('50863', None)
        self.table.setSortingEnabled(False) 
        self.columnList = self.columnList + resultDict["column"]
        self.table.setColumnCount(len(self.columnList))
        self.table.setHorizontalHeaderLabels(self.columnList)
        self.table.clearContents()
        self.table.setRowCount(len(resultDict["rs"]))
        self.renderTable(resultDict["rs"])
        self.table.setSortingEnabled(True)
        self.table.resizeRowsToContents() 
        
    def renderTable(self, tableList):
        isBold = False
        for indexRow, listItem in enumerate(tableList):
            for indexColumn, item in enumerate(listItem):
                self.addItemtoTable2(self.table,item,indexRow,indexColumn, isBold)
           
            
