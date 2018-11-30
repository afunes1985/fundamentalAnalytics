'''
Created on 4 nov. 2018

@author: afunes
'''
import dash
from dash.dependencies import Output, Input, State
import dash_table
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.dao import Dao, DaoCompanyResult
import dash_core_components as dcc
import dash_html_components as html
from testPlot import testPlot
from web.app import app

#Initializer()
#rs = Dao.getCompanyList()
#rows = rs.fetchall()
#if (len(rows) != 0):
#    df = DataFrame(rows)
#    df.columns = rs.keys()
#df = DataFrame()
layout = html.Div([
    html.H1('Page 1'),
    html.Div(dcc.Input(), style={'display': 'none'}),
    html.Button(id='submit-button1', n_clicks=0, children='Submit'),
    dash_table.DataTable(
        id='datatable-companyList',
        columns=[
            {"name": i, "id": i, "deletable": True} for i in []
        ],
        data=[],
        editable=True,
        filtering=True,
        sorting=True,
        sorting_type="multi",
        row_selectable="multi",
        selected_rows=[],
    ),
    html.Div(id='datatable-companyList-container')
])

# @app.callback(
#     Output('datatable-companyList-container', "children"),
#     [Input('submit-button1', 'n_clicks')],
#     [State('datatable-companyList', "derived_virtual_data"),
#      State('datatable-companyList', "selected_rows")])
# def doSubmit(n_clicks, rows, selected_rows):
#     print(rows)
#     print(selected_rows)
#     if (len(selected_rows) != 0):
#         for selected_row in selected_rows:
#             print(rows[selected_row]) 