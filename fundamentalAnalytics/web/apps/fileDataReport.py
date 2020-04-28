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

fileStatusList = FileDataDao().getFileStatusList()
ddDict = []
for row in fileStatusList:
        ddDict.append({'label': row[0], 'value': row[0]})
ddFileStatus = dcc.Dropdown(
    id='dd-fileStatus',
    value=None,
    clearable=False,
    options=ddDict
)

layout = dbc.Container([
            dbc.Row([dbc.Col(html.Label(["File Status", ddFileStatus], style={'width':'100%'}), width=2),
                dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'))]),
            dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-fileDataReport')), html.Div(id='dt-fileDataReportContainer')])],
            style={"max-width":"95%"})

@app.callback(
    Output('dt-fileDataReportContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dd-fileStatus', 'value')])
def executeReport(n_clicks, fileStatus):
    if(n_clicks > 0):
        rs2 = FileDataDao().getFileDataForReport(fileStatus)
        df2 = DataFrame(rs2)
        dt2 = dt.DataTable(
            id='dt-fileDataReport',
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
                'textAlign': 'left'
            }]
        )
        return dt2