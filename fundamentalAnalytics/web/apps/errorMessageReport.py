'''
Created on 4 nov. 2018

@author: afunes
'''

import dash
from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from dao.fileDataDao import FileDataDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dataImport.importerExecutor import ImporterExecutor
from engine.fileDataEngine import FileDataEngine
from valueobject.constant import Constant
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
            dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-errorMessage')), html.Div(id='dt-errorMessageContainer')]),
            dbc.Row([dbc.Col(dbc.Button(id='btn-selectAll', n_clicks=0, children='Select All')),
                     dbc.Col(dcc.RadioItems(id="rb-action",
                        options=[
                            {'label': 'Process', 'value': 'Process'},
                            {'label': 'Reprocess', 'value': 'Reprocess'},
                            {'label': 'Delete', 'value': 'Delete'}
                        ], value='Reprocess',
                        labelStyle={'display': 'inline-block'})),
                     dbc.Col(dbc.Button(id='btn-executeImporter', n_clicks=0, children='Process')),
            ])
        ])

@app.callback(
    Output('dt-errorMessageContainer', "children"),
    [Input('btn-executeReport', 'n_clicks'),
     Input('btn-executeImporter', 'n_clicks')],
    [State('dt-errorMessage', "derived_virtual_data"),
     State('dt-errorMessage', "derived_virtual_selected_rows"),
     State('dd-errorKey', 'value'),
     State('rb-action', 'value')])
def executeReport(n_clicks, n_clicks2, rows, selected_rows, errorKey, action):
    if(n_clicks > 0):
        ctx = dash.callback_context
        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if(button_id == 'btn-executeReport'):
            return refreshDT(errorKey)
        elif(button_id == 'btn-executeImporter'):
            return importeFileData(rows, selected_rows, errorKey, action)


@app.callback(
    Output('dt-errorMessage', "selected_rows"),
    [Input('btn-selectAll', 'n_clicks')],
    [State('dt-errorMessage', "derived_virtual_data")])
def select_all(n_clicks, rows):
    if(n_clicks > 0):
        return [i for i, row in enumerate(rows)]
    else:
        return []
            
def refreshDT(errorKey):
    status = Constant().getErrorKeyDict()[errorKey]['status']
    rs2 = FileDataDao().getErrorMessageGroup(errorKey, status)
    df2 = DataFrame(rs2)
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


    
def importeFileData(rows, selected_rows, errorKey, action):
    if (selected_rows != None and len(selected_rows) >= 1):
        for selected_row in selected_rows:
            errorMessage = rows[selected_row]["errorMessage"]
            fileDataList = FileDataDao().getFileDataByError(errorKey=errorKey, errorMessage=errorMessage)
            importerClass = Constant().getErrorKeyDict()[errorKey]['importerClass']
            FileDataEngine().processFileData2(action=action, fileDataList=fileDataList, importerClass=importerClass)
        return refreshDT(errorKey)
