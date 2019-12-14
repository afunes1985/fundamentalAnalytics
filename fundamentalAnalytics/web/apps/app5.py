from dash.dependencies import Input, Output, State

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
from dataImport.importerExecutor import ImporterExecutor
from importer.importerFile import ImporterFile
import plotly.graph_objects as go
from web.app import app


rs = FileDataDao().getStatusCount()
dictImportStatus = {'labels':[], 'parents':[], 'values':[], 'ids':[]}

for row in rs:
    dictImportStatus['labels'].append(row.label)
    dictImportStatus['parents'].append(row.parent)
    dictImportStatus['values'].append(row.values_)
    dictImportStatus['ids'].append(row.id)
    
dictImportStatus['ids'] = dictImportStatus['labels'] + dictImportStatus['ids'] 
dictImportStatus['ids'] = list(dict.fromkeys(dictImportStatus['ids']))
#dictImportStatus['labels'] = list(dict.fromkeys(dictImportStatus['labels']))
dictImportStatus['ids'] = [ x for x in dictImportStatus['ids'] if x is not None ]

print(dictImportStatus)

sunburstImportStatus = go.Figure(go.Sunburst(
    labels=dictImportStatus['labels'],
    parents=dictImportStatus['parents'],
    values=dictImportStatus['values'],
    ids=dictImportStatus['ids'],
    #branchvalues='total',
    marker=dict(
        colorscale='RdBu',
        cmid=1),
        ))

sunburstImportStatus.update_layout(margin = dict(t=0, l=0, r=0, b=0))

listDDImportStatus = []
for importStatus in [ x for x in list(dict.fromkeys(dictImportStatus['parents'])) if x is not None ]:
    print(importStatus)
    listDDImportStatus.append({'label': importStatus, 'value': importStatus})

ddImportStatus = dcc.Dropdown(
    id='dd-importStatus',
    options=listDDImportStatus,
    value=None,
    clearable=False
)

layout = html.Div([
                dcc.Graph(
                    id='graph',
                    figure = sunburstImportStatus,
                    style={'width': '70%', 'display': 'inline-block', 'float': 'left'}),
                html.Div(id='fig-importStatus', style={'clear': 'both'}),
                html.Label(["Massive execution", ddImportStatus]),
                html.Button(id='btn-submit-processFile', n_clicks=0, children='Submit'),
                html.Div(id='hidden2-div', style={'display':'none'})
                ])

@app.callback(
    Output('hidden2-div', "children"),
    [Input('btn-submit-processFile', 'n_clicks')],
    [State('dd-importStatus', 'value')])
def doSubmitProcessFile(n_clicks, value):
    if (n_clicks > 0):
        print(value)
        session = DBConnector().getNewSession()
        fileDataList = FileDataDao().getFileData2(statusAttr='importStatus', statusValue =value, session = session)
        importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFile)
        importerExecutor.execute(fileDataList)
        


