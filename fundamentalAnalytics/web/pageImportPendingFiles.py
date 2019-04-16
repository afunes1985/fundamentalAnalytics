'''
Created on 4 nov. 2018

@author: afunes
'''
import dash
from dash.dependencies import Output, Input, State
from pandas.core.frame import DataFrame

from base.initializer import Initializer
from dao.dao import Dao, DaoCompanyResult, FileDataDao
import dash_html_components as html
import dash_table_experiments as dt
from testPlot import testPlot
from valueobject.valueobject import FilterFactVO


Initializer()
rs = FileDataDao.getFileDataList()
if (len(rs) != 0):
    df = DataFrame(rs)
   # df.columns = rs.keys()
app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True
app.layout = html.Div([
    dt.DataTable(
            rows=df.to_dict("rows"),
            row_selectable=True,
            filterable=True,
            sortable=True,
            selected_row_indices=[],
            editable = False,
            id='datatable-fileDataList'
    ),
    html.Div(id='datatable-fileDataList-container'),
    html.Button(id='submit-button1', n_clicks=0, children='Submit'),
    html.Div(id='hidden-div', style={'display':'none'})
])

@app.callback(
    Output('datatable-fileDataList-container', "children"),
    [Input('submit-button1', 'n_clicks')],
    [State('datatable-fileDataList', "rows"),
     State('datatable-fileDataList', "selected_row_indices")])
def doSubmit(n_clicks, rows, selected_row_indices):
    print('Test')


if __name__ == '__main__':
    app.run_server(debug=True, port=8051)
    