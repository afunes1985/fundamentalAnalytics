'''
Created on 4 nov. 2018

@author: afunes
'''
from datetime import datetime

import dash
from dash.dependencies import Input, Output, State
import dash_table
import pandas
from pandas.core.frame import DataFrame

from dao.dao import DaoCompanyResult
import dash_core_components as dcc
import dash_html_components as html
from testPlot import testPlot

def getTableValues(CIK, ticker, conceptName2):
    rs = DaoCompanyResult.getFactValues2(ticker = ticker, conceptName = conceptName2, periodType = None)
    rows = rs.fetchall()
    df = DataFrame(columns=['reportName', 'conceptName'])
    rows_list = []
    rowDict = None
    for row in rows:
        reportName = row[0]
        conceptName = row[1]
        if(rowDict is None):
            rowDict = {}
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
        if(rowDict.get('conceptName', None) != conceptName):
            rows_list.append(rowDict)
            rowDict = {}
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
        reportDate = row[4].strftime('%d-%m-%Y')
        rowDict[reportDate] = row[3]
    
    df = DataFrame(rows_list, columns=rowDict.keys())
    df = df.sort_values(["reportName", "conceptName"], ascending=[True,True])

def initPage(app, selectedCompanies):
    df = getTableValues(selectedCompanies[0]["CIK"], None, None)
    app.layout = html.Div([
        # embed `dcc` input in initial layout (https://github.com/plotly/dash-renderer/issues/46)
        html.Div(dcc.Input(), style={'display': 'none'}),
        html.Button(id='submit-button', n_clicks=0, children='Submit'),
        dash_table.DataTable(
            id='datatable-interactivity',
            columns=[
                {"name": i, "id": i, "deletable": True} for i in df.columns
            ],
            data=df.to_dict("rows"),
            editable=True,
            filtering=True,
            sorting=True,
            sorting_type="multi",
            row_selectable="multi",
            row_deletable=True,
            selected_rows=[],
        ),
        html.Div(id='datatable-interactivity-container')
    ])

@app.callback(
    Output('datatable-interactivity-container', "children"),
    [Input('submit-button', 'n_clicks')],
    [State('datatable-interactivity', "derived_virtual_data"),
     State('datatable-interactivity', "selected_rows")])
def doSubmit(n_clicks, rows, selected_rows):
    print(rows)
    print(selected_rows)
    if (len(selected_rows) != 0):
        testPlot("INTC", [rows[selected_rows[0]]["conceptName"]])

