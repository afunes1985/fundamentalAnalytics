import logging

from dash.dependencies import Input, Output

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from tools.tools import createLog
from valueobject.constant import Constant
from web.app import app
from web.apps import  entityFactReport, ccrcReportApp, factReportApp, fileDataSumApp, \
    fileMassiveImporterApp, errorMessageReport, fileDataReport


dropDownMenu = dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(dcc.Link("Custom Concept Rel Concept", href="/apps/ccrcReportApp")),
                dbc.DropdownMenuItem(dcc.Link("Entity Fact Report", href="/apps/entityFactReport")),
                dbc.DropdownMenuItem(dcc.Link("Error Message Report", href="/apps/errorMessageReport")),
                dbc.DropdownMenuItem(dcc.Link("File Data Report", href="/apps/fileDataReport"))
            ],
            #nav=True,
            in_navbar=True,
            label="More"
        )

navbar = dbc.Navbar(
    [
        html.A(
            dbc.Row(
                [
                    dbc.Col(dbc.NavbarBrand("Fundalytics", className="ml-2")),
                    dbc.Col(dcc.Link("Fact Report", href="/apps/factReportApp")),
                    dbc.Col(dcc.Link("File Data Summary", href="/apps/fileDataSumApp")),
                    dbc.Col(dcc.Link("File Massive Importer", href="/apps/fileMassiveImporterApp")),
                    dbc.Col(dropDownMenu)
                ],
                align="center",
                justify="end"
            )
        ),
    ],
    color="dark",
    dark=True,
)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='navBar', children=navbar, style={"max-width":"100%"}),
    html.Div(id='page-content')
])

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/fileDataSumApp':
        return fileDataSumApp.layout
    elif pathname == '/apps/ccrcReportApp':
        return ccrcReportApp.layout
    elif pathname == '/apps/entityFactReport':
        return entityFactReport.layout
    elif pathname == '/apps/fileMassiveImporterApp':
        return fileMassiveImporterApp.layout
    elif pathname == '/apps/factReportApp':
        return factReportApp.layout
    elif pathname == '/apps/errorMessageReport':
        return errorMessageReport.layout
    elif pathname == '/apps/fileDataReport':
        return fileDataReport.layout
    else:
        return factReportApp.layout

if __name__ == '__main__':
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    app.run_server(debug=True)