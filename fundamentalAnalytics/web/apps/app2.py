'''
Created on 4 nov. 2018

@author: afunes
'''
import subprocess
import webbrowser

import dash
from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from engine.factEngine import FactEngine
from importer.importerCalculate import ImporterCalculate
from importer.importerCopy import ImporterCopy
from importer.importerExpression import ImporterExpression
from importer.importerFact import ImporterFact
from importer.importerFile import ImporterFile
from importer.importerPrice import ImporterPrice
from web.app import app
from importer.importerEntityFact import ImporterEntityFact


layout = html.Div([
    dcc.Input(id='txt-filename', value='', type='text'),
    dcc.Input(id='txt-ticker', value='', type='text'),
    html.Button(id='btn-submit', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(data=[{}], id='dt-fileData'), style={'display': 'none'}),
    html.Div(id='dt-fileData-container'),
    dcc.RadioItems(id="rb-action",
        options=[
            {'label': 'Process', 'value': 'Process'},
            {'label': 'Reprocess', 'value': 'Reprocess'},
            {'label': 'Delete', 'value': 'Delete'}
        ], value='Reprocess',
        labelStyle={'display': 'inline-block'}),
    html.Button(id='btn-reprocess-file', n_clicks=0, children='File'),
    html.Button(id='btn-reprocess-fact', n_clicks=0, children='Fact'),
    html.Button(id='btn-reprocess-entity', n_clicks=0, children='Entity'),
    html.Button(id='btn-reprocess-price', n_clicks=0, children='Price'),
    html.Button(id='btn-reprocess-copy', n_clicks=0, children='Copy'),
    html.Button(id='btn-reprocess-calculate', n_clicks=0, children='Calculate'),
    html.Button(id='btn-reprocess-expression', n_clicks=0, children='Expression'),
    html.Button(id='btn-goToSECURL', n_clicks=0, children='Go To SEC'),
    html.Button(id='btn-goToDir', n_clicks=0, children='Go To DIR'),
    html.Button(id='btn-showError', n_clicks=0, children='Show Errors'),
    html.Div(dt.DataTable(data=[{}], id='dt-errorMessage'), style={'display': 'none'}),
    html.Div(id='dt-errorMessage-container'),
    dcc.Link('Go to Fact Report', href='/apps/app1'),
    html.Div(id='hidden-div3', style={'display':'none'}),
    html.Div(id='hidden-div4', style={'display':'none'})
])


@app.callback(
    output=Output('dt-fileData-container', "children"),
    inputs=[Input('btn-submit', 'n_clicks'),Input('btn-reprocess-calculate', 'n_clicks'),Input('btn-reprocess-fact', 'n_clicks'), Input('btn-reprocess-entity', 'n_clicks'),
            Input('btn-reprocess-file', 'n_clicks'),Input('btn-reprocess-price', 'n_clicks'),Input('btn-reprocess-copy', 'n_clicks'),Input('btn-reprocess-expression', 'n_clicks')],
    state=[State('txt-filename', "value"),State('txt-ticker', "value"), State('dt-fileData', "derived_virtual_data"),
                    State('dt-fileData', "derived_virtual_selected_rows"),State('rb-action', 'value')])
def doButtonAction(n_clicks,n_clicks2,n_clicks3,n_clicks4,n_clicks5,n_clicks6,n_clicks7,n_clicks8, filename, ticker, rows, selected_rows, rbValue):
    if (n_clicks > 0):
        ctx = dash.callback_context
        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        print(button_id)
        if(button_id == 'btn-reprocess-calculate'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterCalculate)
        elif(button_id == 'btn-reprocess-fact'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterFact)
        elif(button_id == 'btn-reprocess-entity'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterEntityFact)
        elif(button_id == 'btn-reprocess-file'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterFile)
        elif(button_id == 'btn-reprocess-price'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterPrice)
        elif(button_id == 'btn-reprocess-copy'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterCopy)
        elif(button_id == 'btn-reprocess-expression'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterExpression)

        return doSubmit(filename, ticker)



def doSubmit(filename, ticker):
    if (filename != '' or ticker != ''):
        rs2 = FileDataDao.getFileDataList3(filename, ticker)
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
    Output('dt-errorMessage-container', "children"),
    [Input('btn-showError', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def showErrors(n_clicks, rows, selected_rows):
    if (n_clicks > 0):
        print(rows)
        print(selected_rows)
        if(selected_rows is not None and len(selected_rows) != 0):
            fileName = rows[selected_rows[0]]["fileName"]
            rs2 = FileDataDao().getErrorList(fileName)
            if (len(rs2) != 0):
                df2 = DataFrame(rs2)
                dt2 = dt.DataTable(
                    id='dt-errorMessage',
                    columns=[
                        {"name": i, "id": i, "deletable": False} for i in df2.columns
                    ],
                    data=df2.to_dict("rows"),
                    ) 
                return dt2
 
@app.callback(
    Output('hidden-div3', "children"),
    [Input('btn-goToSECURL', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doGoToSECURL(n_clicks, rows, selected_rows):  
    if (n_clicks > 0):  
        if(selected_rows is not None and len(selected_rows) != 0):
            fileName = rows[selected_rows[0]]["fileName"]
            fileName = fileName.replace(".txt", "-index.htm")
            url = 'https://www.sec.gov/Archives/' + fileName
            webbrowser.open(url) 
            
@app.callback(
    Output('hidden-div4', "children"),
    [Input('btn-goToDir', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doGoToDir(n_clicks, rows, selected_rows):  
    if (n_clicks > 0):  
        if(selected_rows is not None and len(selected_rows) == 0):
            fileName = rows[selected_rows[0]]["fileName"]
            fileName = fileName.replace(".txt", "\\")
            fileName = fileName.replace("/", "\\")
            url = "D:\\Per\\cache\\" + fileName
            print(url)
            subprocess.Popen(r'explorer /select,"' + url + '"')

def doAction(n_clicks, rows, selected_rows, rbValue, importerClass):
    if (n_clicks > 0):  
        if (len(rows) != 0 and len(rows) < 50):#Protect massive running
            if(selected_rows is not None and len(selected_rows) != 0):
                for rowIndex in selected_rows:
                    fileName = rows[rowIndex]["fileName"]
                    if rbValue == 'Reprocess':
                        importer = importerClass(filename=fileName, replace=True)
                        importer.doImport()
                    elif rbValue == 'Process':
                        importer = importerClass(filename=fileName, replace=False)
                        importer.doImport()
                    elif rbValue == 'Delete':
                        importer = importerClass(filename=fileName, replace=True)
                        importer.deleteImportedObject()
                        importer.addOrModifyFDPending()
            else:
                for row in rows:
                    fileName = row["fileName"]
                    importer = importerClass(filename=fileName, replace=True)
                    if rbValue == 'Reprocess':
                        importer = importerClass(filename=fileName, replace=True)
                        importer.doImport()
                    elif rbValue == 'Process':
                        importer = importerClass(filename=fileName, replace=False)
                        importer.doImport()
                    elif rbValue == 'Delete':
                        importer = importerClass(filename=fileName, replace=True)
                        importer.deleteImportedObject()
                        importer.addOrModifyFDPending()
            
