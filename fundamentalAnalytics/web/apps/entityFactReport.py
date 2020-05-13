'''
Created on 4 nov. 2018

@author: afunes
'''

from dash.dependencies import Output, Input, State
from dash.exceptions import PreventUpdate
from pandas.core.frame import DataFrame

from dao.companyDao import CompanyDao
from dao.dao import Dao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from tools.tools import getNumberValueAsString
from web.app import app

options = CompanyDao().getAllTicker()
options2 = []
for o in options:
    options2.append({"label":o[0], "value":o[0]})

layout = dbc.Container([
    dbc.Row([dbc.Col([html.Label("Company Tickers"),
                          dcc.Dropdown(id="dd-companyTicker", multi=True)],
                          width=2),
            dbc.Col([html.Label("Company list"), dt.DataTable(data=[{}], id='dt-companyData2'), html.Div(id='dt-companyDataContainer2')], width=4),
            dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'), align='end')],
            justify="center", style={'margin':10}),
    dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-entityFactList'), style={'display': 'none'}),
             html.Div(id='dt-entityFactListContainer')]),
             html.Div(id='hidden-div', style={'display':'none'})],
    style={"max-width":"95%"})

def getFactValues(CIK):
    rs = Dao().getValuesForApp4(CIK = CIK)
    rows = rs.fetchall()
    rows_list = []
    rowDict = {}
    columnNameForDate = []
    for row in rows:
        conceptName = row.conceptName
        value = getNumberValueAsString(row.value)
        valueDate = row.endDate
        explicitMemberValue = row.explicitMemberValue
        key = conceptName +"-"+str(explicitMemberValue)
        if(rowDict.get('key', None) != key):
            if(rowDict.get('key', None) is not None):
                rows_list.append(rowDict)
            rowDict = {} 
            rowDict['key'] = key
            rowDict["conceptName"] = conceptName
            rowDict["explicitMemberValue"] = explicitMemberValue
        rowDict[valueDate.strftime('%d-%m-%Y')] = value
        if valueDate not in columnNameForDate:
            columnNameForDate.append(valueDate)
    rows_list.append(rowDict)     
    columnKeys = ['conceptName', 'explicitMemberValue']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    
    df = DataFrame(rows_list, columns=columnKeys)
    df = df.sort_values(["conceptName"], ascending=[True])
    return df

@app.callback(
    Output('dt-companyDataContainer2', "children"),
    [Input("dd-companyTicker", "value")])
def updateDTCompanyData(values): 
    if(values is not None and len(values) > 0):
        rows = CompanyDao().getCompanyListByTicker(values)
        if (len(rows) != 0):
            df = DataFrame(rows)
            dtCompanyData = dt.DataTable(
                        id='dt-companyData2',
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
    Output('dt-entityFactListContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dt-companyData2', "derived_virtual_data")])
def doSubmit(n_clicks, rows):
    if (rows is not None and len(rows) != 0 and len(rows[0]) != 0):
        for row in rows:
            df2 = getFactValues(CIK=row["CIK"])
            dt2 = dt.DataTable(
                id='dt-entityFactList',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df2.columns
                ],
                data=df2.to_dict("rows"),
                filter_action="native",
                sort_action="native",
                page_action = 'none',
                style_cell={
                    'minWidth': '90px', 'maxWidth': '220px',
                    'whiteSpace': 'no-wrap',
                    'textOverflow': 'ellipsis',
                    'overflow': 'hidden',
                })
            return dt2