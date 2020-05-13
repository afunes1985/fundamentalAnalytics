'''
Created on 4 nov. 2018

@author: afunes
'''

from dash.dependencies import Output, Input
from pandas.core.frame import DataFrame

from dao.customFactDao import CustomFactDao
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table as dt
from web.app import app


layout = dbc.Container([
            dbc.Row(dbc.Col(dbc.Button(id='btn-executeReport', n_clicks=0, children='Submit'), align='end')),
            dbc.Row([html.Div(dt.DataTable(data=[{}], id='dt-ccRelationConcept')), html.Div(id='dt-ccRelationConceptContainer')])])


@app.callback(
    Output('dt-ccRelationConceptContainer', "children"),
    [Input('btn-executeReport', 'n_clicks')])
def executeCCRCReport(n_clicks):
    if (n_clicks > 0):
        rs2 = CustomFactDao().getCConceptAndConcept()
        df2 = DataFrame(rs2)
        dt2 = dt.DataTable(
            id='dt-ccRelationConcept',
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