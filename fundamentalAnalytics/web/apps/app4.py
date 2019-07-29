'''
Created on 4 nov. 2018

@author: afunes
'''

from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from dao.dao import Dao
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from web.app import app
from web.apps.app1 import getNumberValueAsString


layout = html.Div([
    dcc.Input(id='txt-ticker', value='', type='text'),
    html.Button(id='btn-submit', n_clicks=0, children='Submit'),
    html.Div(dt.DataTable(data=[{}], id='dt-entityFactList'), style={'display': 'none'}),
    html.Div(id='dt-entityFactList-container'),
    dcc.Link('Go to App1', href='/apps/app1'),
    html.Div(id='hidden-div', style={'display':'none'})])

def getFactValues(ticker):
    rs = Dao().getValuesForApp4(ticker = ticker)
    rows = rs.fetchall()
    rows_list = []
    rowDict = {}
    columnNameForDate = []
    for row in rows:
        conceptName = row[0]
        value = getNumberValueAsString(row[1])
        valueDate = row[2]
        #print(conceptName + " " + str(value) + " " + str(valueDate))
        if(rowDict.get('conceptName', None) != conceptName):
            if(rowDict.get('conceptName', None) is not None):
                rows_list.append(rowDict)
            rowDict = {} 
            rowDict['conceptName'] = conceptName
        rowDict[valueDate.strftime('%d-%m-%Y')] = value
        if valueDate not in columnNameForDate:
            columnNameForDate.append(valueDate)
    rows_list.append(rowDict)     
    columnKeys = ['reportName', 'conceptName', 'periodType', 'order']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    
    df = DataFrame(rows_list, columns=columnKeys)
    df = df.sort_values(["conceptName"], ascending=[True])
    return df

@app.callback(
    Output('dt-entityFactList-container', "children"),
    [Input('btn-submit', 'n_clicks')],
    [State('txt-ticker', "value")])
def doSubmit(n_clicks, ticker):
    if (ticker is not None):
            df2 = getFactValues(ticker)
            dt2 = dt.DataTable(
                id='dt-entityFactList',
                columns=[
                    {"name": i, "id": i, "deletable": False} for i in df2.columns
                ],
                data=df2.to_dict("rows"),
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                row_selectable="multi",
                page_action = 'none',
                style_table={'overflowX': 'scroll'},
                fixed_rows={ 'headers': True, 'data': 0 },
                style_cell={
                    'minWidth': '90px', 'maxWidth': '220px',
                    'whiteSpace': 'no-wrap',
                    'textOverflow': 'ellipsis',
                    'overflow': 'hidden',
                },
#                 css=[{
#                     'selector': '.dash-cell div.dash-cell-value',
#                     'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
#                 }],
                style_cell_conditional=[
                     {'if': {'column_id': 'reportName'},
                         'textAlign': 'left'},
                     {'if': {'column_id': 'conceptName'},
                         'textAlign': 'left'}
                 ])
            return dt2