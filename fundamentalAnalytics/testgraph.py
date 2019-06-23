import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv')
# add an id column and set it as the index
# in this case the unique ID is just the country name, so we could have just
# renamed 'country' to 'id' (but given it the display name 'country'), but
# here it's duplicated just to show the more general pattern.
df['id'] = df['country']
df.set_index('id', inplace=True, drop=False)

app = dash.Dash(__name__)

app.layout = html.Div([
    dash_table.DataTable(
        id='datatable-row-ids',
        columns=[
            {"name": i, "id": i, "deletable": False} for i in df.columns
        ],
        data=df.to_dict("rows"),
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        row_selectable="multi",
        style_table={'overflowX': 'scroll'},
        fixed_rows={ 'headers': True, 'data': 0 },
        style_cell_conditional=[
            {'if': {'column_id': 'pop'},
                'width': '300px',
                'whiteSpace': 'no-wrap'},
        ]
    ),
    html.Div(id='datatable-row-ids-container')
])



if __name__ == '__main__':
    app.run_server(debug=True)