'''
Created on 22 ago. 2018

@author: afunes
'''
from engine.calculator import Calculator
import plotly.graph_objs as go
import plotly.plotly as py

resultDict = Calculator.calculateBookValue("INTC")

data = [go.Scatter(
        x=resultDict["x"],
        y=resultDict["y"])]
py.iplot(data, filename = resultDict["indicatorID"])
