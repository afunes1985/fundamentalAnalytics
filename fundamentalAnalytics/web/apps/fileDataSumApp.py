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
from importer.importerCompany import ImporterCompany
import dash_bootstrap_components as dbc

layout = dbc.Container([
        dbc.Row([
                dbc.Col([dbc.Row(html.Label("File name")),
                         dbc.Row(dcc.Input(id='txt-filename', value='', type='text'))],
                         width=2),
                dbc.Col([html.Label("Company Tickers"),
                          dcc.Dropdown(id="dd-companyTicker", multi=True)],
                         width=2),
                 dbc.Col([html.Label("Company list"), dt.DataTable(data=[{}], id='dt-companyData'), html.Div(id='dt-companyDataContainer')], width=4),
                 dbc.Col(dbc.Button(id='btn-submit', n_clicks=0, children='Submit'), align='end')],
                 justify="center", style={'margin':10}),
        dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-fileData'), style={'display': 'none'}),
                html.Div(id='dt-fileData-container', style={'margin': 10})]),
        dcc.RadioItems(id="rb-action",
            options=[
                {'label': 'Process', 'value': 'Process'},
                {'label': 'Reprocess', 'value': 'Reprocess'},
                {'label': 'Delete', 'value': 'Delete'}
            ], value='Reprocess',
            labelStyle={'display': 'inline-block'}),
        dbc.Button(id='btn-reprocess-file', n_clicks=0, children='File', style={"margin":2}),
        dbc.Button(id='btn-reprocess-company', n_clicks=0, children='Company', style={"margin":2}),
        dbc.Button(id='btn-reprocess-entity', n_clicks=0, children='Entity', style={"margin":2}),
        dbc.Button(id='btn-reprocess-price', n_clicks=0, children='Price', style={"margin":2}),
        dbc.Button(id='btn-reprocess-fact', n_clicks=0, children='Fact', style={"margin":2}),
        dbc.Button(id='btn-reprocess-copy', n_clicks=0, children='Copy', style={"margin":2}),
        dbc.Button(id='btn-reprocess-calculate', n_clicks=0, children='Calculate', style={"margin":2}),
        dbc.Button(id='btn-reprocess-expression', n_clicks=0, children='Expression', style={"margin":2}),
        dbc.Button(id='btn-goToSECURL', n_clicks=0, children='Go To SEC', style={"margin":2}),
        dbc.Button(id='btn-goToDir', n_clicks=0, children='Go To DIR', style={"margin":2}),
        dbc.Button(id='btn-showError', n_clicks=0, children='Show Errors'),
        html.Div(dt.DataTable(data=[{}], id='dt-errorMessage'), style={'display': 'none'}),
        html.Div(id='dt-errorMessage-container'),
        html.Div(id='hidden-div3', style={'display':'none'}),
        html.Div(id='hidden-div4', style={'display':'none'})
        ], style={"max-width":"95%"}
    )


@app.callback(
    output=Output('dt-fileData-container', "children"),
    inputs=[Input('btn-submit', 'n_clicks'), Input('btn-reprocess-calculate', 'n_clicks'), Input('btn-reprocess-fact', 'n_clicks'), Input('btn-reprocess-entity', 'n_clicks'), Input('btn-reprocess-company', 'n_clicks'),
            Input('btn-reprocess-file', 'n_clicks'), Input('btn-reprocess-price', 'n_clicks'), Input('btn-reprocess-copy', 'n_clicks'), Input('btn-reprocess-expression', 'n_clicks')],
    state=[State('txt-filename', "value"), State('dt-companyData', "derived_virtual_data"), State('dt-fileData', "derived_virtual_data"),
                    State('dt-fileData', "derived_virtual_selected_rows"), State('rb-action', 'value')])
def doButtonAction(n_clicks, n_clicks2, n_clicks3, n_clicks4, n_clicks5, n_clicks6, n_clicks7, n_clicks8, n_clicks9, filename, tickerRows, rows, selected_rows, rbValue):
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
        elif(button_id == 'btn-reprocess-company'):
            doAction(n_clicks, rows, selected_rows, rbValue, ImporterCompany)

        return doSubmit(filename, tickerRows)


def doSubmit(filename, tickerRows):
    if (filename != '' or (len(tickerRows) != 0 and len(tickerRows[0]) != 0)):
        for row in tickerRows:
            if(len(tickerRows[0]) != 0):
                rs2 = FileDataDao().getFileDataList4(ticker=row["ticker"])
            else:
                rs2 = FileDataDao().getFileDataList3(filename=filename)
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
                    row_selectable="multi"
                    ) 
                return dt2


@app.callback(
    Output('dt-errorMessage-container', "children"),
    [Input('btn-showError', 'n_clicks')],
    [State('dt-fileData', "derived_virtual_data"),
     State('dt-fileData', "derived_virtual_selected_rows")])
def showErrors(n_clicks, rows, selected_rows):
    if (n_clicks > 0):
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
        if(selected_rows is not None and len(selected_rows) != 0):
            fileName = rows[selected_rows[0]]["fileName"]
            fileName = fileName.replace(".txt", "\\")
            fileName = fileName.replace("/", "\\")
            url = "D:\\Per\\cache\\" + fileName
            print(url)
            subprocess.Popen(r'explorer /select,"' + url + '"')


def doAction(n_clicks, rows, selected_rows, rbValue, importerClass):
    if (n_clicks > 0):  
        if (len(rows) != 0 and len(rows) < 120):  # Protect massive running
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
            
