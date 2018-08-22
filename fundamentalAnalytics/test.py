'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame
from sqlalchemy.sql.expression import text

from base.dbConnector import DbConnector
import plotly.graph_objs as go
import plotly.plotly as py


dbconnector = DbConnector()
session = dbconnector.getNewSession()


with dbconnector.engine.connect() as con:
    #indicatorID = 'CashAndCashEquivalentsAtCarryingValue'
    params = { 'indicatorID' : 'CashAndCashEquivalentsAtCarryingValue',
               'companyID' : '1318605'}
    query = text("SELECT * FROM company_q_fundamental where indicatorID = :indicatorID and companyID = :companyID")
    rs = con.execute(query, params)
    df = DataFrame(rs.fetchall())
    #data = [go.Scatter(
    #    x=rs.keys(),
    #    y=df.values.tolist()[0])]
    #py.iplot(data)
    print(df)
