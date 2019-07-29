'''
Created on 4 nov. 2018

@author: afunes
'''

from pandas.core.frame import DataFrame
import dash_table as dt
import dash_core_components as dcc
import dash_html_components as html
from dao.customFactDao import CustomFactDao

rs2 = CustomFactDao().getCConceptAndConcept()
df2 = DataFrame(rs2)
layout = html.Div([
    html.Div(dt.DataTable(
                    id='dt-ccRelationConcept',
                    columns=[
                        {"name": i, "id": i, "deletable": False} for i in df2.columns
                    ],
                    data=df2.to_dict("rows"),
                    filter_action="native",
                    sort_action="native",
                    sort_mode="multi",
                    row_selectable="multi",
                    )),
    html.Div(id='dt-ccRelationConcept-container'),
    dcc.Link('Go to Fact Report', href='/apps/app1')])
