from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from pandas.core.frame import DataFrame

from base.dbConnector import DBConnector
from dao.companyDao import CompanyDao
from dao.factDao import FactDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from engine.expressionEngine import ExpressionEngine
from engine.plotlyEngineInterface import PlotlyEngineInterface
from tools.tools import getNumberValueAsString
from valueobject.valueobject import FilterFactVO
from web.app import app

options = CompanyDao().getAllTicker()
options2 = []
for o in options:
    options2.append({"label":o[0], "value":o[0]})

btnGroup=dbc.ButtonGroup(
    [
        dbc.Col(dbc.Button(id='btn-submit', style={'margin': 5}, className="fa fa-refresh")),
        dbc.Col(dbc.Button(id='btn-sendPlotyData', style={'margin': 5}, className="fa fa-line-chart"))
    ],
    vertical=True,
)

layout = dbc.Container(
    [
        dbc.Row([dbc.Col([html.Label("Company Tickers"),
                          dcc.Dropdown(id="dd-companyTicker", multi=True),
                          dcc.RadioItems(
                                id="ri-CustomOrFact",
                                options=[
                                    {'label': 'Custom', 'value': 'Custom'},
                                    {'label': 'Fact', 'value': 'Fact'},
                                    {'label': 'Both', 'value': 'Both'}
                                ],
                                value='Custom',
                                style = {'margin' : 2}
                            )], width=2),
                 dbc.Col([html.Label("Company list"), dt.DataTable(data=[{}], id='dt-companyData'), html.Div(id='dt-companyDataContainer')], width=4)],
                 style={'margin':10}),
        dbc.Row([dbc.Col(btnGroup, width={"size": "1px"}),
                 dbc.Col(html.Div(id='dt-factReportContainer', style={'width':'100%'}), width=11),
                 dbc.Col(html.Div(dt.DataTable(data=[{}], id='dt-factReport'), style={'display': 'none'}), width={"size": 0})],
                 no_gutters=True,
                 style={'margin':0}),
        dbc.Row(html.Div(id='hidden-div2', style={'display':'none'}))
    ], style={"max-width":"100%"}
)

 
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
    if(values is not None and len(values) > 0):
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
    [Input('btn-submit', 'n_clicks')],
    [State('dt-companyData', "derived_virtual_data"),
     State('ri-CustomOrFact', "value")])
def executeFactReport(n_clicks, rows, riValue):
    if (rows is not None and len(rows) != 0 and len(rows[0]) != 0):
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
                row_selectable="multi",
                style_cell={
                    'minWidth': 'auto', 'maxWidth': '220px',
                    'whiteSpace': 'no-wrap',
                    'textOverflow': 'ellipsis',
                    'overflow': 'hidden',
                },
                fixed_columns={'headers': True, 'data': 2}, 
                style_table={'overflowX': 'auto', 
                             'minWidth': '900px', 'width': '100%', 'maxWidth': '1450px'},
                #style_data={ 'border': '1px grey' },
#                 css= [{'selector': 'table',   TRY TO EXPAND THE TABLE AT 100% of SCREEN
#                        'rule': 'width: 100%;'}]
            )
            return dt2
    else:
        raise PreventUpdate

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
    rs = FactDao.getFactValues2(CIK=CIK, customOrFact=customOrFact)
    rows = rs.fetchall()
    tableDict = {}
    rowDict = {}
    columnNameForDate = []
    oldRowID = None
    for row in rows:
        reportName = row.reportShortName[0:70]
        conceptName = row.conceptName
        value = getNumberValueAsString(row.value)
        valueDate = row.date_
        periodType = row.periodType
        order = row.order_
        newRowID = reportName +"-"+ conceptName +"-"+ periodType
#         print('newRowID', newRowID)
        if(oldRowID is None or newRowID != oldRowID):
            #Changing row
#             print('oldRowID', oldRowID)
            if(rowDict.get('conceptName', None) is not None):
                tableDict[oldRowID] = rowDict
            oldRowID = newRowID
            rowDict = {} 
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
            rowDict['periodType'] = periodType
            rowDict['order'] = order
        rowDict[valueDate.strftime('%d-%m-%Y')] = value
        if valueDate not in columnNameForDate:
            columnNameForDate.append(valueDate)
    tableDict[newRowID] = rowDict
    columnKeys = ['reportName', 'conceptName', 'periodType', 'order']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    columnKeys.append('CURRENT')
    setCurrentFactValues(tableDict=tableDict, CIK=CIK, ticker=ticker)
    df = DataFrame(list(tableDict.values()), columns=columnKeys)
    df = df.sort_values(["reportName", "order"], ascending=[True, True])
#     print(df.to_string())
    app.CIK = CIK
    app.ticker = ticker
    return df

def setCurrentFactValues(tableDict, CIK, ticker):
    ee = ExpressionEngine()
    session = DBConnector(isNullPool=True).getNewSession()
    rList = ee.solveCurrentExpression(CIK = CIK, ticker=ticker, session=session)
    for cfVO in rList:
        rowID = "CUSTOM_RATIO" +"-"+ cfVO.customConcept.conceptName +"-"+ cfVO.periodType
        print(rowID, cfVO.value)
        tableDict[rowID]["CURRENT"] = getNumberValueAsString(cfVO.value)
        tableDict[rowID]['reportName'] = "CUSTOM_RATIO"
        tableDict[rowID]['conceptName'] = cfVO.customConcept.conceptName
        tableDict[rowID]['periodType'] = cfVO.periodType
        tableDict[rowID]['order'] = cfVO.order_
