'''
Created on 4 nov. 2018

@author: afunes
'''
import dash
from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.dao import Dao, DaoCompanyResult
import dash_html_components as html
import dash_table_experiments as dt
from web.app import app


Initializer()
rs = Dao.getCompanyList()
rows = rs.fetchall()
if (len(rows) != 0):
    df = DataFrame(rows)
    df.columns = rs.keys()
#app = dash.Dash(__name__)
#app.config.suppress_callback_exceptions = True
layout = html.Div([
    html.Button(id='submit-button1', n_clicks=0, children='Submit'),
    dt.DataTable(
            rows=df.to_dict("rows"),
            row_selectable=True,
            filterable=True,
            sortable=True,
            selected_row_indices=[],
            editable = False,
            id='datatable-companyList'
    ),
    html.Div(id='datatable-companyList-container')
])

@app.callback(
    Output('datatable-companyList', "children"),
    [Input('submit-button1', 'n_clicks')],
    [State('datatable-companyList', "rows"),
     State('datatable-companyList', "selected_row_indices")])
def doSubmit(n_clicks, rows, selected_row_indices):
    if (selected_row_indices is not None and len(selected_row_indices) != 0):
        for selected_row in selected_row_indices:
            df2 = getTableValues(rows[selected_row]["CIK"], rows[selected_row]["ticker"], None)
            dt2 = dt.DataTable(
                rows=df2.to_dict("rows"),
                row_selectable=True,
                filterable=True,
                sortable=True,
                selected_row_indices=[],
                id='datatable-factList',
                min_width = 3000,
                editable = False,
                column_widths = {0 : 300, 1 : 300})
            return dt2
        
def getTableValues(CIK, ticker, conceptName2):
    rs = DaoCompanyResult.getFactValues2(CIK = CIK, ticker = ticker, conceptName = None)
    rows = rs.fetchall()
    df = DataFrame(columns=['reportName', 'conceptName'])
    rows_list = []
    rowDict = {}
    columnNameForDate = []
    for row in rows:
        reportName = row[0]
        conceptName = row[1]
        value = getNumberValueAsString(row[3])
        reportDate = row[4]
        periodType = row[5]
        order = row[6]
        if(rowDict.get('conceptName', None) != conceptName or rowDict.get('periodType', None) != periodType):
            if(rowDict.get('conceptName', None) is not None):
                rows_list.append(rowDict)
            rowDict = {} 
            rowDict['reportName'] = reportName
            rowDict['conceptName'] = conceptName
            rowDict['periodType'] = periodType
            rowDict['order'] = order
        rowDict[reportDate.strftime('%d-%m-%Y')] = value
        if reportDate not in columnNameForDate:
            columnNameForDate.append(reportDate)
    rows_list.append(rowDict)     
    columnKeys = ['reportName', 'conceptName', 'periodType', 'order']
    columnNameForDate.sort()
    columnNameForDate = (x.strftime('%d-%m-%Y') for x in columnNameForDate)
    columnKeys.extend(columnNameForDate)
    
    df = DataFrame(rows_list, columns=columnKeys)
    df = df.sort_values(["reportName", "conceptName"], ascending=[True,True])
    return df
 
def getNumberValueAsString(value):
    if(value % 1):
        return value
    else:
        return str(int(value/1000000)) + " M" 