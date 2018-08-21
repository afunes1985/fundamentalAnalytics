'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame

from base.dbConnector import DbConnector
import plotly.graph_objs as go
import plotly.plotly as py


dbconnector = DbConnector()
session = dbconnector.getNewSession()


with dbconnector.engine.connect() as con:
    query = "SELECT * FROM company_q_fundamental where indicatorID = 'CashAndCashEquivalentsAtCarryingValue' and companyID = '1318605'"
    rs = con.execute(query)
    df = DataFrame(rs.fetchall())
    df.columns = rs.keys()
    data = [go.Scatter(
          x=rs.keys(),
          y=df.values.tolist()[0])]
    py.iplot(data)
    print(df)
