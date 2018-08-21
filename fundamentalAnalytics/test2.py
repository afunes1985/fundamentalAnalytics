'''
Created on 21 ago. 2018

@author: afunes
'''

import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv")

data = [go.Scatter(
          x=df.Date,
          y=df['AAPL.Close'])]

py.iplot(data)