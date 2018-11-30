'''
Created on 4 nov. 2018

@author: afunes
'''

import dash
from dash.dependencies import Input, Output, State
import dash_table
import flask
from pandas.core.frame import DataFrame
from werkzeug.utils import redirect

from base.initializer import Initializer
from dao.dao import Dao
import dash_core_components as dcc
import dash_html_components as html


app = dash.Dash(
    __name__, 
    external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css']
)

app.config.suppress_callback_exceptions = True

url_bar_and_content_div = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


Initializer()
rs = Dao.getCompanyList()
rows = rs.fetchall()
if (len(rows) != 0):
    df = DataFrame(rows)
    df.columns = rs.keys()

html.Div([
    html.H1('Page 1'),
    html.Div(dcc.Input(), style={'display': 'none'}),
    html.Button(id='submit-button1', n_clicks=0, children='Submit'),
    
    html.Div(id='datatable-companyList-container'),
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

layout_index = html.Div([
    html.H1('Page 1'),
    html.Div(dcc.Input(), style={'display': 'none'}),
    html.Button(id='submit-button1', n_clicks=0, children='Submit'),
    dash_table.DataTable(
        id='datatable-companyList',
        columns=[
            {"name": i, "id": i, "deletable": True} for i in df.columns
        ],
        data=df.to_dict("rows"),
        editable=True,
        filtering=True,
        sorting=True,
        sorting_type="multi",
        row_selectable="multi",
        selected_rows=[],
    ),
    html.Div(id='datatable-companyList-container')
])


@app.callback(
    Output('datatable-companyList-container', "children"),
    [Input('submit-button1', 'n_clicks')],
    [State('datatable-companyList', "derived_virtual_data"),
     State('datatable-companyList', "selected_rows")])
def doSubmit(n_clicks, rows, selected_rows):
    print(rows)
    print(selected_rows)
    if (selected_rows is not None and len(selected_rows) != 0):
        for selected_row in selected_rows:
            print(rows[selected_row]) 
            app.layout = layout_page_1


# @app.callback(
#     Output('datatable-companyList-container', "children"),
#     [Input('submit-button1', 'n_clicks')],
#     [State('datatable-companyList', "derived_virtual_data"),
#      State('datatable-companyList', "selected_rows")])
# def doSubmit(n_clicks, rows, selected_rows):
#     print(rows)
#     print(selected_rows)
#     if (selected_rows is not None and len(selected_rows) != 0):
#         for selected_row in selected_rows:
#             print(rows[selected_row]) 
#             app.layout = layout_page_1
#            

layout_page_1 = html.Div([
    html.H2('Page 1'),
    dcc.Input(id='input-1-state', type='text', value='Montreal'),
    dcc.Input(id='input-2-state', type='text', value='Canada'),
    html.Button(id='submit-button', n_clicks=0, children='Submit'),
    html.Div(id='output-state'),
    html.Br(),
    dcc.Link('Navigate to "/"', href='/'),
])


# def serve_layout():
#     if flask.has_request_context():
#         return url_bar_and_content_div
#     return html.Div([
#         url_bar_and_content_div,
#         layout_index,
#         layout_page_1,
#     ])


app.layout = layout_index

# 
# # Index callbacks
# @app.callback(Output('page-content', 'children'),
#               [Input('url', 'pathname')])
# def display_page(pathname):
#     if pathname == "/page-1":
#         return layout_page_1
#     else:
#         return layout_index

# 
# # Page 1 callbacks
# @app.callback(Output('output-state', 'children'),
#               [Input('submit-button', 'n_clicks')],
#               [State('input-1-state', 'value'),
#                State('input-2-state', 'value')])
# def update_output(n_clicks, input1, input2):
#     return ('The Button has been pressed {} times,'
#             'Input 1 is "{}",'
#             'and Input 2 is "{}"').format(n_clicks, input1, input2)


if __name__ == '__main__':
    app.run_server(debug=False)