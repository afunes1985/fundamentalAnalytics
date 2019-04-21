from dash import dash
from dash.dependencies import Input, Output, State
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.dao import Dao
from dao.factDao import FactDao
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from engine.plotlyEngineInterface import PlotlyEngineInterface
from testPlot import testPlot
from valueobject.valueobject import FilterFactVO
from web.app import app


Initializer()
rs = Dao.getCompanyList()
rows = rs.fetchall()
if (len(rows) != 0):
    df = DataFrame(rows)
    df.columns = rs.keys()
layout = html.Div([
    dt.DataTable(
            rows=df.to_dict("rows"),
            row_selectable=True,
            filterable=True,
            sortable=True,
            selected_row_indices=[],
            editable = False,
            min_height = 200,
            id='datatable-companyList'
    ),
    html.Div(id='datatable-companyList-container'),
    html.Button(id='btn-submit-showFact', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(rows=[{}], id='datatable-factList'), style={'display': 'none'}),
    html.Div(id='datatable-factList-container'),
    html.Button(id='btn-submit-sendPlotlyData', n_clicks=0, children='Submit'),
    dcc.Link('Go to Fact Import', href='/apps/app2'),
    html.Div(id='hidden-div', style={'display':'none'})
])

@app.callback(
    Output('datatable-factList-container', "children"),
    [Input('btn-submit-showFact', 'n_clicks')],
    [State('datatable-companyList', "rows"),
     State('datatable-companyList', "selected_row_indices")])
def doSubmitShowFact(n_clicks, rows, selected_row_indices):
    if (selected_row_indices is not None and len(selected_row_indices) != 0):
        for selected_row in selected_row_indices:
            df2 = getFactValues(rows[selected_row]["CIK"], rows[selected_row]["ticker"], None)
            dt2 = dt.DataTable(
                rows=df2.to_dict("rows"),
                row_selectable=True,
                filterable=True,
                sortable=True,
                selected_row_indices=[],
                id='datatable-factList',
                min_width = 3500,
                min_height = 500,
                editable = False,
                column_widths = {0 : 300, 1 : 300})
            return dt2
 
def getFactValues(CIK, ticker, conceptName2):
    rs = FactDao.getFactValues2(CIK = CIK, ticker = ticker, conceptName = None)
    rows = rs.fetchall()
    df = DataFrame(columns=['reportName', 'conceptName'])
    rows_list = []
    rowDict = {}
    columnNameForDate = []
    for row in rows:
        reportName = row[0]
        conceptName = row[1]
        value = getNumberValueAsString(row[3])
        reportDate = row[4]
        periodType = row[5]
        order = row[6]
        if(rowDict.get('conceptName', None) != conceptName or rowDict.get('periodType', None) != periodType):
            if(rowDict.get('conceptName', None) is not None):
                rows_list.append(rowDict)
            rowDict = {} 
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
            rowDict['periodType'] = periodType
            rowDict['order'] = order
        rowDict[reportDate.strftime('%d-%m-%Y')] = value
        if reportDate not in columnNameForDate:
            columnNameForDate.append(reportDate)
    rows_list.append(rowDict)     
    columnKeys = ['reportName', 'conceptName', 'periodType', 'order']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    
    df = DataFrame(rows_list, columns=columnKeys)
    df = df.sort_values(["reportName", "conceptName"], ascending=[True,True])
    app.ticker = ticker
    return df
 
  
@app.callback(
    Output('hidden-div', "children"),
    [Input('btn-submit-sendPlotlyData', 'n_clicks')],
    [State('datatable-factList', "rows"),
     State('datatable-factList', "selected_row_indices")])
def doSubmitSendPlotlyData(n_clicks, rows, selected_row_indices):
    if (selected_row_indices != None and len(selected_row_indices) != 0):
        filterFactVOList = []
        for selected_row in selected_row_indices:
            filterFactVO = FilterFactVO()
            filterFactVO.conceptName = rows[selected_row]["conceptName"]
            filterFactVO.reportShortName = rows[selected_row]["reportName"]
            filterFactVO.ticker = app.ticker
            filterFactVO.periodType = rows[selected_row]["periodType"]
            filterFactVOList.append(filterFactVO)
        PlotlyEngineInterface.sendToPlotly(filterFactVOList)
  
def getNumberValueAsString(value):
    if(value % 1):
        return value
    else:
        return str(int(value/1000000)) + " M" 
