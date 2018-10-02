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


def getTraceData(ticker, concept):
    rs = DaoCompanyResult.getFactValues(ticker = ticker, conceptName = concept.conceptName)
    rows = rs.fetchall()
    if (len(rows) != 0):
        df = DataFrame(rows)
        df.columns = rs.keys()
        trace = go.Scatter(
            x=df["date_"],
            y=df["value"],
            name = concept.conceptName)
        return trace
    else:
        raise Exception("No data found " + concept.conceptID)

data = []   
ticker = 'TSLA'
filename = ticker
Initializer()
#listConceptID = ['NetIncomeLoss']
#BALANCE
listConceptID = ['CashAndCashEquivalentsAtCarryingValue', 'AssetsCurrent', 'Assets', 'LiabilitiesCurrent', 'StockholdersEquity']
#listConceptID = ['OperatingExpenses', 'OperatingIncomeLoss', 'NetIncomeLoss', 'CashAndCashEquivalentsPeriodIncreaseDecrease', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest']
#CASH FLOW
#listConceptID = ['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations','NetCashProvidedByUsedInFinancingActivitiesContinuingOperations']

#listConceptID = ['NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInInvestingActivities','NetCashProvidedByUsedInFinancingActivities']
for conceptName in listConceptID:
    concept = GenericDao.getFirstResult(Concept, Concept.conceptName == conceptName)
    data.append(getTraceData(ticker, concept))
    filename = filename #+ " " + concept.label
print(filename)   
layout = go.Layout(
    title=filename[0:100]
)
fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename = filename[0:100])
