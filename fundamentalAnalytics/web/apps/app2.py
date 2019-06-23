'''
Created on 4 nov. 2018

@author: afunes
'''
import webbrowser

from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame
import dash_table as dt
from base.initializer import Initializer
from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
from dataImport.importFactFromSEC import initMainCache
from engine.factEngine import FactEngine
from engine.factImporterEngine import FactImporterEngine
from engine.importFileEngine import ImportFileEngine
from web.app import app


Initializer()

layout = html.Div([
    dcc.Input(id='txt-filename', value='', type='text'),
    html.Button(id='btn-submit', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(data=[{}], id='dt-fileData'), style={'display': 'none'}),
    html.Div(id='dt-fileData-container'),
    html.Button(id='btn-reimport', n_clicks=0, children='Reimport'),
    html.Button(id='btn-reprocess', n_clicks=0, children='Reprocess'),
    html.Button(id='btn-delete', n_clicks=0, children='Delete'),
    html.Button(id='btn-goToSECURL', n_clicks=0, children='GO TO SEC'),
    dcc.Link('Go to Fact Report', href='/apps/app1'),
    html.Div(id='hidden-div2', style={'display':'none'}),
    html.Div(id='hidden-div1', style={'display':'none'}),
    html.Div(id='hidden-div3', style={'display':'none'}),
    html.Div(id='hidden-div4', style={'display':'none'})
])

@app.callback(
    Output('dt-fileData-container', "children"),
    [Input('btn-submit', 'n_clicks')],
    [State('txt-filename', "value")])
def doSubmit(n_clicks, value):
    if (value is not None and value != ''):
        rs2 = FileDataDao.getFileDataList3(value)
        if (len(rs2) != 0):
            df2 = DataFrame(rs2)
            dt2 = dt.DataTable(
                id='dt-fileData',
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

@app.callback(
    Output('hidden-div1', "children"),
    [Input('btn-reprocess', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doReprocess(n_clicks, rows, selected_rows):
    print(rows)
    print(selected_rows)
    if(selected_rows is not None and len(selected_rows) != 0):
        mainCache = initMainCache()
        fileName = rows[selected_rows[0]]["fileName"]
        print(fileName)
        fi = FactImporterEngine(fileName, True, mainCache)
        fi.doImport()
 
@app.callback(
    Output('hidden-div2', "children"),
    [Input('btn-delete', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doDelete(n_clicks, rows, selected_rows):
    print(selected_rows)
    if(selected_rows is not None and len(selected_rows) != 0):
        fileName = rows[selected_rows[0]]["fileName"]
        print(fileName)
        FactEngine.deleteFactByFileData(fileName)
 
 
@app.callback(
    Output('hidden-div3', "children"),
    [Input('btn-reimport', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doReImport(n_clicks, rows, selected_rows):
    print(rows)
    print(selected_rows)
    if(selected_rows is not None and len(selected_rows) != 0):
        fileName = rows[selected_rows[0]]["fileName"]
        print(fileName)
        ImportFileEngine.importFiles(filename=fileName, reimport=True)
 
@app.callback(
    Output('hidden-div4', "children"),
    [Input('btn-goToSECURL', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doGoToSECURL(n_clicks, rows, selected_rows):    
    print(rows)
    print(selected_rows)
    if(selected_rows is not None and len(selected_rows) != 0):
        fileName = rows[selected_rows[0]]["fileName"]
        fileName = fileName.replace(".txt", "-index.htm")
        url = 'https://www.sec.gov/Archives/' + fileName
        webbrowser.open(url) 
