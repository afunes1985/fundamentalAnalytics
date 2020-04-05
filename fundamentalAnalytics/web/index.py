import logging

from dash.dependencies import Input, Output

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from tools.tools import createLog
from valueobject.constant import Constant
from web.app import app
from web.apps import  app3, app4, factReportApp, fileDataSumApp, \
    fileMassiveImporterApp


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dcc.Link("Fact Report", href="/apps/factReportApp"), style={"margin-left" : 10}),
        dbc.NavItem(dcc.Link("File Data Summary", href="/apps/fileDataSumApp", style={"margin-left" : 10})),
        dbc.NavItem(dcc.Link("File Massive Importer", href="/apps/fileMassiveImporterApp", style={"margin-left" : 10})),
    ],
    brand="Fundalytics",
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
    elif pathname == '/apps/app3':
        return app3.layout
    elif pathname == '/apps/app4':
        return app4.layout
    elif pathname == '/apps/fileMassiveImporterApp':
        return fileMassiveImporterApp.layout
    elif pathname == '/apps/factReportApp':
        return factReportApp.layout
    else:
        return factReportApp.layout

if __name__ == '__main__':
    logging.info("START")
    createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
    app.run_server(debug=True)