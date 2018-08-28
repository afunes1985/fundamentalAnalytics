'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame

from base.dbConnector import DBConnector
from base.initializer import Initializer
from dao.dao import DaoCompanyResult, GenericDao
from modelClass.concept import Concept
import plotly.graph_objs as go
import plotly.plotly as py


def getTrace(data, fileName, ticker, indicatorID):
    rs = DaoCompanyResult.getCompanyResult(None, ticker, indicatorID)
    concept = GenericDao.getFirstResult(Concept, Concept.conceptID == indicatorID)
    rows = rs.fetchall()
    if (len(rows) != 0):
        df = DataFrame(rows)
        df.columns = rs.keys()
        trace = go.Scatter(
            x=df["yq"],
            y=df["value"],
            name = concept.label)
        data.append(trace)
        fileName = fileName + " " + concept.label


data = []   
ticker = 'INTC'
filename = ticker
Initializer()
#listIndicatorID = ['SalesRevenueNet', 'CostOfGoodsAndServicesSold', 'GrossProfit']
listIndicatorID = ['OperatingExpenses', 'OperatingIncomeLoss', 'NetIncomeLoss', 'CashAndCashEquivalentsPeriodIncreaseDecrease', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest']
#listIndicatorID = ['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations','NetCashProvidedByUsedInFinancingActivitiesContinuingOperations']
for indicatorID in listIndicatorID:
    getTrace(data, filename, ticker, indicatorID)    
py.iplot(data, filename = filename[0:100])
