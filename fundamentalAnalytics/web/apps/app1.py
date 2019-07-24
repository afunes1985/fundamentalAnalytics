from dash.dependencies import Input, Output, State
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.companyDao import CompanyDao
from dao.factDao import FactDao
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from engine.plotlyEngineInterface import PlotlyEngineInterface
from valueobject.valueobject import FilterFactVO
from web.app import app



Initializer()
rs = CompanyDao().getCompanyList()
rows = rs.fetchall()
if (len(rows) != 0):
    df = DataFrame(rows)
    df.columns = rs.keys() 
    layout = html.Div([dt.DataTable(
                id='dt-companyList',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df.columns
                ],
                data=df.to_dict("rows"),
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                row_selectable="multi",
                page_current= 0,
                page_size= 10,
        ),
    html.Div(id='dt-companyList-container'),
    html.Button(id='btn-submit-showFact', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(data=[{}], id='dt-factList'), style={'display': 'none'}),
    html.Div(id='dt-factList-container'),
    html.Button(id='btn-submit-sendPlotlyData', n_clicks=0, children='Submit'),
    dcc.Link('Go to Fact Import', href='/apps/app2'),
    html.Div(id='hidden-div', style={'display':'none'})
])

@app.callback(
    Output('dt-factList-container', "children"),
    [Input('btn-submit-showFact', 'n_clicks')],
    [State('dt-companyList', "derived_virtual_data"),
     State('dt-companyList', "derived_virtual_selected_rows")])
def doSubmitShowFact(n_clicks, rows, selected_rows):
    if (selected_rows is not None and len(selected_rows) != 0):
        for selected_row in selected_rows:
            df2 = getFactValues(rows[selected_row]["CIK"], rows[selected_row]["ticker"])
            dt2 = dt.DataTable(
                id='dt-factList',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df2.columns
                ],
                data=df2.to_dict("rows"),
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                row_selectable="multi",
                style_table={'overflowX': 'scroll'},
                fixed_rows={ 'headers': True, 'data': 0 },
                style_cell={
                    'minWidth': '90px', 'maxWidth': '220px',
                    'whiteSpace': 'no-wrap',
                    'textOverflow': 'ellipsis',
                    'overflow': 'hidden',
                },
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell_conditional=[
                     {'if': {'column_id': 'reportName'},
                         'textAlign': 'left'},
                     {'if': {'column_id': 'conceptName'},
                         'textAlign': 'left'}
                 ])
            return dt2
 
def getFactValues(CIK, ticker):
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
        #print(conceptName + " " + str(value) + " " + str(reportDate))
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
    [State('dt-factList', "derived_virtual_data"),
     State('dt-factList', "derived_virtual_selected_rows")])
def doSubmitSendPlotlyData(n_clicks, rows, selected_rows):
    if (selected_rows != None and len(selected_rows) != 0):
        filterFactVOList = []
        for selected_row in selected_rows:
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
        intLen = len(str(int(value)))
        if(intLen < 4):
            return value
        elif(intLen < 7):
            return str(int(value/1000)) + " m" 
        elif(intLen >= 7):
            return str(int(value/1000000)) + " M" 
        # elif(intLen < 13):
        #    return str(int(value/1000000000)) + " B"
        else:
            value