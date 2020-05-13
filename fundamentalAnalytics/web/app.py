'''
Created on 4 nov. 2018

@author: afunes
'''
import dash
import dash_bootstrap_components as dbc

external_stylesheets = [dbc.themes.BOOTSTRAP]
# external_stylesheets = ['C://Users//afunes//git//fundamentalAnalytics//fundamentalAnalytics//css//bootstrap.min.css']
# external_stylesheets = [dbc.themes.SOLAR]
# external_stylesheets = ['https://stackpath.bootstrapcdn.com/bootswatch/4.4.1/solar/bootstrap.min.css']
# external_stylesheets = ['https://codepen.io/anon/pen/mardKv.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True
app.ticker = None
app.CIK = None
app.title = 'Fundalytics'
