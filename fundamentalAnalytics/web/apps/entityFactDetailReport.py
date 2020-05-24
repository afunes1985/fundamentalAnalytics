'''
Created on 4 nov. 2018

@author: afunes
'''

from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from dao.companyDao import CompanyDao
from dao.entityFactDao import EntityFactDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from web.app import app


options = CompanyDao().getAllTicker()
options2 = []
for o in options:
    options2.append({"label":o[0], "value":o[0]})

layout = dbc.Container([
    dbc.Row([dbc.Col([html.Label("Company Tickers"),
                          dcc.Dropdown(id="dd-companyTicker", multi=True)],
                          width=2),
            dbc.Col([html.Label("Company list"), dt.DataTable(data=[{}], id='dt-companyData3'), html.Div(id='dt-companyDataContainer3')], width=4),
            dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'), align='end')],
            justify="center", style={'margin':10}),
    dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-entityFactDetail'), style={'display': 'none'}),
             html.Div(id='dt-entityFactDetailContainer')]),
             html.Div(id='hidden-div', style={'display':'none'})],
    style={"max-width":"95%"})


@app.callback(
    Output('dt-companyDataContainer3', "children"),
    [Input("dd-companyTicker", "value")])
def updateDTCompanyData(values): 
    if(values is not None and len(values) > 0):
        rows = CompanyDao().getCompanyListByTicker(values)
        if (len(rows) != 0):
            df = DataFrame(rows)
            dtCompanyData = dt.DataTable(
                        id='dt-companyData3',
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
    Output('dt-entityFactDetailContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dt-companyData3', "derived_virtual_data")])
def executeReport(n_clicks, rows):
    if (rows is not None and len(rows) != 0 and len(rows[0]) != 0):
        for row in rows:
            rs2 = EntityFactDao().getEntityFactValueForReport(CIK=row["CIK"])
            df2 = DataFrame(rs2)
            dt2 = dt.DataTable(
            id='dt-entityFactDetail',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df2.columns
                ],
                data=df2.to_dict("rows"),
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                row_selectable="multi",
            )
            return dt2
