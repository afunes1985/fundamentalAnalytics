import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from pandas.core.frame import DataFrame

from dao.companyDao import CompanyDao
from dao.factDao import FactDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from engine.plotlyEngineInterface import PlotlyEngineInterface
from tools.tools import getNumberValueAsString
from valueobject.valueobject import FilterFactVO
from web.app import app


options = CompanyDao().getAllTicker()
options2 = []
for o in options:
    options2.append({"label":o[0], "value":o[0]})
 
layout = dbc.Container(
    [
        dbc.Row([dbc.Col(html.Label(["Company Tickers",dcc.Dropdown(id="dd-companyTicker", multi=True)],style={"width": "90%"}), width=2),
                 dbc.Col(html.Label(["Company list",dt.DataTable(data=[{}], id='dt-companyData'), html.Div(id='dt-companyDataContainer',style={"width": "90%"})],style={"width": "90%"}), width=4)],
#                  no_gutters=True,
                 justify="center"),
        dbc.Row([dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'))]),
        dbc.Row(dcc.RadioItems(
            id="ri-CustomOrFact",
            options=[
                {'label': 'Custom', 'value': 'Custom'},
                {'label': 'Fact', 'value': 'Fact'},
                {'label': 'Both', 'value': 'Both'}
            ],
            value='Custom'
        )),
        dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-factReport')),html.Div(id='dt-factReportContainer')]),
        dbc.Row([dbc.Col(dbc.Button(id='btn-sendPlotyData', n_clicks=0, children='Submit'))]),
        dbc.Row(html.Div(id='hidden-div2', style={'display':'none'}))
    ], style= {"max-width":"95%"}
)
app.layout = layout
 
@app.callback(
    Output("dd-companyTicker", "options"),
    [Input("dd-companyTicker", "search_value")],
    [State("dd-companyTicker", "value")]
)
def updateDDCompanyTicker(search_value, value):
    if not search_value or len(search_value) < 2:
        raise PreventUpdate
    returnList = [
        o for o in options2 if search_value in o["label"] or o["value"] in (value or [])
    ]
    return returnList

@app.callback(
    Output('dt-companyDataContainer', "children"),
    [Input("dd-companyTicker", "value")])
def updateDTCompanyData(values): 
    if(values is not None and len(values)>0):
        rows = CompanyDao().getCompanyListByTicker(values)
        if (len(rows) != 0):
            df = DataFrame(rows)
            dtCompanyData = dt.DataTable(
                        id='dt-companyData',
                        columns=[
                            {"name": i, "id": i, "deletable": False} for i in df.columns
                        ],
                        data=df.to_dict("rows"),
                        sort_action="native",
                        sort_mode="multi",
                        style_as_list_view=True,
                )
            return dtCompanyData

@app.callback(
    Output('dt-factReportContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dt-companyData', "derived_virtual_data"),
     State('ri-CustomOrFact', "value")])
def executeFactReport(n_clicks, rows, riValue):
    if (rows is not None and len(rows) != 0):
        for row in rows:
            df2 = getFactValues(CIK=row["CIK"], ticker=row["ticker"], customOrFact=riValue)
            dt2 = dt.DataTable(
                id='dt-factReport',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df2.columns
                ],
                data=df2.to_dict("rows"),
                filter_action="native",
                sort_action="native",
#                 sort_mode="multi",
                row_selectable="multi",
#                 page_action = 'none',
                style_cell={
                    'minWidth': '110px', 'maxWidth': '220px',
                    'whiteSpace': 'no-wrap',
                    'textOverflow': 'ellipsis',
                    'overflow': 'hidden',
                },
#                 virtualization=True
                )
            return dt2

@app.callback(
    Output('hidden-div2', "children"),
    [Input('btn-sendPlotyData', 'n_clicks')],
    [State('dt-factReport', "derived_virtual_data"),
     State('dt-factReport', "derived_virtual_selected_rows")])
def doSubmitSendPlotlyData(n_clicks, rows, selected_rows):
    if (selected_rows != None and len(selected_rows) != 0):
        filterFactVOList = []
        for selected_row in selected_rows:
            filterFactVO = FilterFactVO()
            filterFactVO.conceptName = rows[selected_row]["conceptName"]
            filterFactVO.reportShortName = rows[selected_row]["reportName"]
            filterFactVO.CIK = app.CIK
            filterFactVO.ticker = app.ticker
            filterFactVO.periodType = rows[selected_row]["periodType"]
            filterFactVOList.append(filterFactVO)
        PlotlyEngineInterface().sendToPlotly(filterFactVOList)
 
def getFactValues(CIK, ticker, customOrFact):
    rs = FactDao.getFactValues2(CIK = CIK, customOrFact = customOrFact)
    rows = rs.fetchall()
    rows_list = []
    rowDict = {}
    columnNameForDate = []
    for row in rows:
        reportName = row[0][0:70]
        conceptName = row[1]
        value = getNumberValueAsString(row[3])
        valueDate = row[4]
        periodType = row[5]
        order = row[6]
        #print(conceptName + " " + str(value) + " " + str(valueDate))
        if(rowDict.get('conceptName', None) != conceptName or rowDict.get('periodType', None) != periodType):
            if(rowDict.get('conceptName', None) is not None):
                rows_list.append(rowDict)
            rowDict = {} 
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
            rowDict['periodType'] = periodType
            rowDict['order'] = order
        rowDict[valueDate.strftime('%d-%m-%Y')] = value
        if valueDate not in columnNameForDate:
            columnNameForDate.append(valueDate)
    rows_list.append(rowDict)     
    columnKeys = ['reportName', 'conceptName', 'periodType', 'order']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    
    df = DataFrame(rows_list, columns=columnKeys)
    df = df.sort_values(["reportName", "conceptName"], ascending=[True,True])
    app.CIK = CIK
    app.ticker = ticker
    return df
