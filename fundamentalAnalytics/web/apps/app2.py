'''
Created on 4 nov. 2018

@author: afunes
'''
import dash
from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from dataImport.importFactFromSEC import initMainCache
from engine.factEngine import FactEngine
from engine.fileImporter import FileImporter
from testPlot import testPlot
from valueobject.valueobject import FilterFactVO
from web.app import app


Initializer()

layout = html.Div([
    dcc.Input(id='txt-filename', value='', type='text'),
    html.Button(id='btn-submit', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(rows=[{}], id='dt-fd2'), style={'display': 'none'}),
    html.Div(id='dt-fd2-container'),
    html.Button(id='btn-reprocess', n_clicks=0, children='Reprocess'),
    html.Button(id='btn-delete', n_clicks=0, children='Delete'),
    dcc.Link('Go to Fact Report', href='/apps/app1'),
    html.Div(id='hidden-div2', style={'display':'none'})
])

@app.callback(
    Output('dt-fd2-container', "children"),
    [Input('btn-submit', 'n_clicks')],
    [State('txt-filename', "value")])
def doSubmit(n_clicks, value):
    if (value is not None and value != ''):
        rs2 = FileDataDao.getFileDataList3(value)
        if (len(rs2) != 0):
            df2 = DataFrame(rs2)
            dt2 = dt.DataTable(
                rows=df2.to_dict("rows"),
                row_selectable=True,
                filterable=True,
                sortable=True,
                selected_row_indices=[],
                editable = False,
                id='dt-fd2'
                ) 
            return dt2

@app.callback(
    Output('btn-reprocess', "children"),
    [Input('btn-reprocess', 'n_clicks')],
    [State('dt-fd2', "rows"),
     State('dt-fd2', "selected_row_indices")])
def doReprocess(n_clicks, rows, selected_row_indices):
    print(rows)
    print(selected_row_indices)
    print ('test')
    if(selected_row_indices is not None and len(selected_row_indices) != 0):
        mainCache = initMainCache()
        fileName = rows[selected_row_indices[0]]["fileName"]
        fi = FileImporter(fileName, True, mainCache)
        fi.doImport()

@app.callback(
    Output('hidden-div2', "children"),
    [Input('btn-delete', 'n_clicks')],
    [State('dt-fd2', "rows"),
     State('dt-fd2', "selected_row_indices")])
def doDelete(n_clicks, rows, selected_row_indices):
    print(selected_row_indices)
    if(selected_row_indices is not None and len(selected_row_indices) != 0):
        fileName = rows[selected_row_indices[0]]["fileName"]
        FactEngine.deleteFactByFileData(fileName)


@app.callback(
    Output('app-2-display-value', 'children'),
    [Input('app-2-dropdown', 'value')])
def display_value(value):
    return 'You have selected "{}"'.format(value)