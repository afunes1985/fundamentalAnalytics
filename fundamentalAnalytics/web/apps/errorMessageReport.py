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
from dataImport.importerExecutor import ImporterExecutor
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
            dbc.Row(dbc.Col(dbc.Button(id='btn-executeImporter', n_clicks=0, children='Submit')))])

@app.callback(
    Output('dt-errorMessageContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')],
    [State('dd-errorKey', 'value')])
def executeReport(n_clicks, errorKey):
    if(n_clicks > 0):
        return refreshDT(errorKey)
        
def refreshDT(errorKey):
        rs2 = FileDataDao().getErrorMessageGroup(errorKey, 'factStatus')
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
    
@app.callback(
    Output('dt-errorMessage', "children"),
    [Input('btn-executeImporter', 'n_clicks')],
    [State('dt-errorMessage', "derived_virtual_data"),
     State('dt-errorMessage', "derived_virtual_selected_rows"),
     State('dd-errorKey', 'value')])
def importeFileData(n_clicks, rows, selected_rows, errorKey):
    if(n_clicks > 0):
        if (selected_rows != None and len(selected_rows) >= 1):
            for selected_row in selected_rows:
                errorMessage = rows[selected_row]["errorMessage"]
                fileDataList = FileDataDao().getFileDataByError(errorKey=errorKey, errorMessage=errorMessage)
                importerClass = Constant().getErrorKeyDict()[errorKey]
                importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=importerClass)
                importerExecutor.execute(fileDataList)
            return refreshDT(errorKey)
        elif(selected_rows != None and len(selected_rows) > 1):
            raise Exception("More than one row selected")