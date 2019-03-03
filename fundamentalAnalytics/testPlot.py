'''
Created on 20 ago. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.dao import DaoCompanyResult, GenericDao
from modelClass.concept import Concept
import plotly.graph_objs as go
import plotly.plotly as py


def getTraceData(reportShortName, ticker, conceptName, periodType):
    rs = DaoCompanyResult.getFactValues2(reportShortName = reportShortName, ticker = ticker, conceptName = conceptName)
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


def testPlot(filterFactVOList):
    data = []   
    #ticker = 'INTC'
    filename = filterFactVOList[0].ticker
    Initializer()
    #listConceptID = ['CashAndCashEquivalentsAtCarryingValue']
    #BALANCE
    #listConceptID = ['CashAndCashEquivalentsAtCarryingValue', 'AssetsCurrent', 'Assets', 'LiabilitiesCurrent', 'StockholdersEquity']
    #listConceptID = ['OperatingExpenses', 'OperatingIncomeLoss', 'NetIncomeLoss', 'CashAndCashEquivalentsAtCarryingValue', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest']
    #CASH FLOW
    #listConceptID = ['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations','NetCashProvidedByUsedInFinancingActivitiesContinuingOperations']
    
    #listConceptID = ['NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInInvestingActivities','NetCashProvidedByUsedInFinancingActivities']
    #listConceptID = ['Revenues', 'CostOfRevenue','GrossProfit', 'OperatingExpenses', 'ProfitLoss', 'NetIncomeLoss']
    
    for filterFactVO in filterFactVOList:
        data.append(getTraceData(filterFactVO.reportShortName, filterFactVO.ticker, filterFactVO.conceptName, None))
        filename = filename + " " + filterFactVO.conceptName
    print(filename)   
    layout = go.Layout(
        title=filename[0:100]
    )
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename = filename[0:100])
