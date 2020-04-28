'''
Created on 4 nov. 2018

@author: afunes
'''

from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from dao.fileDataDao import FileDataDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from web.app import app

errorKeyList = FileDataDao().getErrorKeyList()
ddDict = []
for row in errorKeyList:
        ddDict.append({'label': row[0], 'value': row[0]})
ddErrorKey = dcc.Dropdown(
    id='dd-errorKey',
    value=None,
    clearable=False,
    options=ddDict
)

layout = dbc.Container([
            dbc.Row([dbc.Col(html.Label(["Error Key", ddErrorKey], style={'width':'100%'}), width=3),
                dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'))]),
            dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-errorMessage')), html.Div(id='dt-errorMessageContainer')])])

@app.callback(
    Output('dt-errorMessageContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dd-errorKey', 'value')])
def executeReport(n_clicks, errorKey):
    if(n_clicks > 0):
        rs2 = FileDataDao().getErrorMessageGroup(errorKey)
        df2 = DataFrame(rs2, columns=["errorMessage", "count"])
        dt2 = dt.DataTable(
            id='dt-errorMessage',
            columns=[
                {"name": i, "id": i, "deletable": False} for i in df2.columns
            ],
            data=df2.to_dict("rows"),
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            row_selectable="multi",
            style_cell_conditional=[
            {
                'if': {'column_id': 'errorMessage'},
                'textAlign': 'left'
            }]
        )
        return dt2