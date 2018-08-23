'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame

import plotly.graph_objs as go
import plotly.plotly as py
from dao.dao import DaoCompanyResult

def getTrace(data, ticker, indicatorID):
    rs = DaoCompanyResult.getCompanyResult(None, ticker, indicatorID)
    rows = rs.fetchall()
    if (len(rows) != 0):
        df = DataFrame(rows)
        df.columns = rs.keys()
        trace = go.Scatter(
            x=df["yq"],
            y=df["value"],
            name = indicatorID)
        data.append(trace)


data = []   
ticker = 'INTC'
filename = ticker
listIndicatorID = ['SalesRevenueNet', 'CostOfGoodsAndServicesSold', 'GrossProfit']
for indicatorID in listIndicatorID:
    getTrace(data, ticker, indicatorID)    
    filename = filename + " " + indicatorID
py.iplot(data, filename = filename)
