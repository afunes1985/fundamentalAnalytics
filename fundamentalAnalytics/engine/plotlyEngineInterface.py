'''
Created on Apr 19, 2019

@author: afunes
'''
'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.factDao import FactDao
import plotly.graph_objects as go
import chart_studio.plotly as py

class PlotlyEngineInterface():
    @staticmethod
    def getTraceData(reportShortName, conceptName, CIK, periodType):
        rs = FactDao.getFactValues2(reportShortName = reportShortName, conceptName = conceptName, CIK=CIK, periodType = periodType)
        rows = rs.fetchall()
        if (len(rows) != 0):
            df = DataFrame(rows)
            df.columns = rs.keys()
            trace = go.Scatter(
                x=df["date_"],
                y=df["value"],
                name = conceptName)
            return trace
        else:
            raise Exception("No data found " + conceptName)
    
    @staticmethod
    def sendToPlotly(filterFactVOList):
        data = []   
        filename = filterFactVOList[0].ticker
        Initializer()
        
        for filterFactVO in filterFactVOList:
            data.append(PlotlyEngineInterface.getTraceData(filterFactVO.reportShortName, filterFactVO.conceptName, filterFactVO.CIK, filterFactVO.periodType))
            filename = filename + " " + filterFactVO.conceptName
        layout = go.Layout(
            title=filename[0:100]
        )
        fig = go.Figure(data=data, layout=layout)
        py.plot(fig, filename = filename[0:100])
