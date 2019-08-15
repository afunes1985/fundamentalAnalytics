'''
Created on 4 nov. 2018

@author: afunes
'''
import subprocess
import webbrowser

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

layout = html.Div([
    dcc.Input(id='txt-filename', value='', type='text'),
    dcc.Input(id='txt-ticker', value='', type='text'),
    html.Button(id='btn-submit', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(data=[{}], id='dt-fileData'), style={'display': 'none'}),
    html.Div(id='dt-fileData-container'),
    dcc.RadioItems(id="rb-action",
        options=[
            {'label': 'Reprocess', 'value': 'Reprocess'},
            {'label': 'Delete', 'value': 'Delete'}
        ], value='Reprocess',
        labelStyle={'display': 'inline-block'}),
    html.Button(id='btn-reprocess-file', n_clicks=0, children='(RP)File'),
    html.Button(id='btn-reprocess-fact', n_clicks=0, children='(RP)Fact'),
    html.Button(id='btn-reprocess-price', n_clicks=0, children='(RP)Price'),
    html.Button(id='btn-reprocess-copy', n_clicks=0, children='(RP)Copy'),
    html.Button(id='btn-reprocess-calculate', n_clicks=0, children='(RP)Calculate'),
    html.Button(id='btn-reprocess-expression', n_clicks=0, children='(RP)Expression'),
    html.Button(id='btn-goToSECURL', n_clicks=0, children='Go To SEC'),
    html.Button(id='btn-goToDir', n_clicks=0, children='Go To DIR'),
    html.Button(id='btn-showError', n_clicks=0, children='Show Errors'),
    html.Div(dt.DataTable(data=[{}], id='dt-errorMessage'), style={'display': 'none'}),
    html.Div(id='dt-errorMessage-container'),
    dcc.Link('Go to Fact Report', href='/apps/app1'),
    html.Div(id='hidden-div2', style={'display':'none'}),
    html.Div(id='hidden-div1', style={'display':'none'}),
    html.Div(id='hidden-div3', style={'display':'none'}),
    html.Div(id='hidden-div4', style={'display':'none'}),
    html.Div(id='hidden-div5', style={'display':'none'}),
    html.Div(id='hidden-div6', style={'display':'none'}),
    html.Div(id='hidden-div7', style={'display':'none'}),
    html.Div(id='hidden-div8', style={'display':'none'})
])


@app.callback(
    Output('dt-fileData-container', "children"),
    [Input('btn-submit', 'n_clicks')],
    [State('txt-filename', "value"),
     State('txt-ticker', "value")])
def doSubmit(n_clicks, filename, ticker):
    if (n_clicks > 0):
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

        
 
#                 FactEngine().deleteFactByFileData(fileName)
  

 
@app.callback(
    Output('hidden-div3', "children"),
    [Input('btn-goToSECURL', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def doGoToSECURL(n_clicks, rows, selected_rows):  
    if (n_clicks > 0):  
        print(rows)
        print(selected_rows)
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


@app.callback(
    Output('hidden-div1', "children"),
    [Input('btn-reprocess-fact', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessFact(n_clicks, rows, selected_rows, rbValue):
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterFact)

@app.callback(
    Output('hidden-div2', "children"),
    [Input('btn-reprocess-file', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessFile(n_clicks, rows, selected_rows, rbValue):
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterFile)
            
@app.callback(
    Output('hidden-div5', "children"),
    [Input('btn-reprocess-price', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessPrice(n_clicks, rows, selected_rows, rbValue):  
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterPrice)

            
@app.callback(
    Output('hidden-div6', "children"),
    [Input('btn-reprocess-copy', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessCopy(n_clicks, rows, selected_rows, rbValue):  
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterCopy)

@app.callback(
    Output('hidden-div7', "children"),
    [Input('btn-reprocess-calculate', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessCalculate(n_clicks, rows, selected_rows, rbValue):  
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterCalculate)
            
@app.callback(
    Output('hidden-div8', "children"),
    [Input('btn-reprocess-expression', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows"),
     State('rb-action', 'value')])
def doReprocessExpression(n_clicks, rows, selected_rows, rbValue):  
    doAction(n_clicks, rows, selected_rows, rbValue, ImporterExpression)

def doAction(n_clicks, rows, selected_rows, rbValue, importerClass):
    if (n_clicks > 0):  
        if(selected_rows is not None and len(selected_rows) != 0):
            for rowIndex in selected_rows:
                fileName = rows[rowIndex]["fileName"]
                if rbValue == 'Reprocess':
                    importerClass(filename=fileName, replace=True).doImport()
                elif rbValue == 'Delete':
                    importerClass(filename=fileName, replace=True).deleteImportedObject()
        else:
            for row in rows:
                fileName = row["fileName"]
                if rbValue == 'Reprocess':
                    importerClass(filename=fileName, replace=True).doImport()
                elif rbValue == 'Delete':
                    importerClass(filename=fileName, replace=True).deleteImportedObject()
