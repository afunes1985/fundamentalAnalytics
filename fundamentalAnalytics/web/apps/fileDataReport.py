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
from engine.fileDataEngine import FileDataEngine
from importer.importerCalculate import ImporterCalculate
from importer.importerCompany import ImporterCompany
from importer.importerCopy import ImporterCopy
from importer.importerEntityFact import ImporterEntityFact
from importer.importerExpression import ImporterExpression
from importer.importerFact import ImporterFact
from importer.importerFile import ImporterFile
from importer.importerPrice import ImporterPrice
from web.app import app


fileStatusDict = FileDataEngine().getFileStatusDict()
ddFileStatus = dcc.Dropdown(
    id='dd-fileStatus',
    value=None,
    options=fileStatusDict
)

companyStatusDict = FileDataEngine().getCompanyStatusDict()
ddCompanyStatus = dcc.Dropdown(
    id='dd-companyStatus',
    value=None,
    options=companyStatusDict
)

entityFactStatusDict = FileDataEngine().getEntityFactStatusDict()
ddEntityFactStatus = dcc.Dropdown(
    id='dd-entityFactStatus',
    value=None,
    options=entityFactStatusDict
)

priceStatusDict = FileDataEngine().getPriceStatusDict()
ddPriceStatus = dcc.Dropdown(
    id='dd-priceStatus',
    value=None,
    options=priceStatusDict
)

factStatusDict = FileDataEngine().getFactStatusDict()
ddFactStatus = dcc.Dropdown(
    id='dd-factStatus',
    value=None,
    options=factStatusDict
)

copyStatusDict = FileDataEngine().getCopyStatusDict()
ddCopyStatus = dcc.Dropdown(
    id='dd-copyStatus',
    value=None,
    options=copyStatusDict
)

calculateStatusDict = FileDataEngine().getCalculateStatusDict()
ddCalculateStatus = dcc.Dropdown(
    id='dd-calculateStatus',
    value=None,
    options=calculateStatusDict
)

expressionStatusDict = FileDataEngine().getExpressionStatusDict()
ddExpressionStatus = dcc.Dropdown(
    id='dd-expressionStatus',
    value=None,
    options=expressionStatusDict
)


layout = dbc.Container([
            dbc.Row([dbc.Col(html.Label(["File Status", ddFileStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Company Status", ddCompanyStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Entity Status", ddEntityFactStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Price Status", ddPriceStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Fact Status", ddFactStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Copy Status", ddCopyStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Calculate Status", ddCalculateStatus], style={'width':'100%'}), width=1),
                     dbc.Col(html.Label(["Expression Status", ddExpressionStatus], style={'width':'100%'}), width=1),
                     dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'))]),
            dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-fileDataReport')), html.Div(id='dt-fileDataReportContainer')]),
            dbc.Row(dcc.RadioItems(id="rb-action",
                options=[
                    {'label': 'Process', 'value': 'Process'},
                    {'label': 'Reprocess', 'value': 'Reprocess'},
                    {'label': 'Delete', 'value': 'Delete'}
                ], value='Reprocess',
                labelStyle={'display': 'inline-block'})),
            dbc.Row([dbc.Button(id='btn-reprocess-file', n_clicks=0, children='File', style={"margin":2}),
                dbc.Button(id='btn-reprocess-company', n_clicks=0, children='Company', style={"margin":2}),
                dbc.Button(id='btn-reprocess-entity', n_clicks=0, children='Entity', style={"margin":2}),
                dbc.Button(id='btn-reprocess-price', n_clicks=0, children='Price', style={"margin":2}),
                dbc.Button(id='btn-reprocess-fact', n_clicks=0, children='Fact', style={"margin":2}),
                dbc.Button(id='btn-reprocess-copy', n_clicks=0, children='Copy', style={"margin":2}),
                dbc.Button(id='btn-reprocess-calculate', n_clicks=0, children='Calculate', style={"margin":2}),
                dbc.Button(id='btn-reprocess-expression', n_clicks=0, children='Expression', style={"margin":2})])
        ],
        style={"max-width":"95%"})

def executeReport(fileStatus, companyStatus, entityFactStatus, priceStatus, factStatus, copyStatus, calculateStatus, expressionStatus):
    rs2 = FileDataDao().getFileDataForReport(fileStatus=fileStatus, companyStatus=companyStatus, entityFactStatus=entityFactStatus, priceStatus=priceStatus, 
                                             factStatus=factStatus, copyStatus=copyStatus, calculateStatus=calculateStatus, expressionStatus=expressionStatus, limit=100)
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
    
@app.callback(
    output=Output('dt-fileDataReportContainer', "children"),
    inputs=[Input('btn-executeReport', 'n_clicks'), Input('btn-reprocess-calculate', 'n_clicks'), Input('btn-reprocess-fact', 'n_clicks'), Input('btn-reprocess-entity', 'n_clicks'), Input('btn-reprocess-company', 'n_clicks'),
            Input('btn-reprocess-file', 'n_clicks'), Input('btn-reprocess-price', 'n_clicks'), Input('btn-reprocess-copy', 'n_clicks'), Input('btn-reprocess-expression', 'n_clicks')],
    state=[State('dt-fileDataReport', "derived_virtual_data"),State('dt-fileDataReport', "derived_virtual_selected_rows"), State('rb-action', 'value'),
                 State('dd-fileStatus', 'value'),State('dd-companyStatus', 'value'), State('dd-entityFactStatus', 'value'),State('dd-priceStatus', 'value'),
                 State('dd-factStatus', 'value'),State('dd-copyStatus', 'value'), State('dd-calculateStatus', 'value'),State('dd-expressionStatus', 'value')])
def doButtonAction(n_clicks, n_clicks2, n_clicks3, n_clicks4, n_clicks5, n_clicks6, n_clicks7, n_clicks8, n_clicks9, rows, selected_rows, rbValue, 
                   fileStatus, companyStatus, entityFactStatus, priceStatus, factStatus, copyStatus, calculateStatus, expressionStatus):
    if (n_clicks > 0):
        ctx = dash.callback_context
        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
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
        return executeReport(fileStatus, companyStatus, entityFactStatus, priceStatus, factStatus, copyStatus, calculateStatus, expressionStatus)
    
def doAction(n_clicks, rows, selected_rows, rbValue, importerClass):
    if (n_clicks > 0):  
        if(selected_rows is not None and len(selected_rows) != 0):
            for rowIndex in selected_rows:
                fileName = rows[rowIndex]["fileName"]
                FileDataEngine().processFileData(action=rbValue, fileName=fileName, importerClass=importerClass)
        else:
            for row in rows:
                fileName = row["fileName"]
                FileDataEngine().processFileData(action=rbValue, fileName=fileName, importerClass=importerClass)