'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame
from sqlalchemy.sql.expression import text

from base.dbConnector import DbConnector
from modelClass.company import Company
import plotly.graph_objs as go
import plotly.plotly as py


dbconnector = DbConnector()
session = dbconnector.getNewSession()

ticker = 'TSLA'
indicatorID = 'CashAndCashEquivalentsAtCarryingValue'

with dbconnector.engine.connect() as con:
    company = session.query(Company)\
    .filter(Company.ticker.__eq__(ticker))\
    .one()

    params = { 'indicatorID' : indicatorID,
               'companyID' : company.companyID}
    query = text("""select CONCAT(year, 'Q', quarter) as yq, value 
                    FROM fa_company_q_result cqr
                    where indicatorID = :indicatorID and companyID = :companyID""")
    rs = con.execute(query, params)
    rows = rs.fetchall()
    if (len(rows) != 0):
        df = DataFrame(rows)
        df.columns = rs.keys()
        data = [go.Scatter(
            x=df["yq"],
            y=df["value"])]
        
        py.iplot(data, filename = company.ticker +"-"+ company.name + " " + indicatorID)
        print(df)
