import logging

from dash.dependencies import Input, Output, State

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
from dataImport.importerExecutor import ImporterExecutor
from importer.importerFile import ImporterFile
import plotly.graph_objects as go
from tools.tools import createLog
from valueobject.constant import Constant
from web.app import app
from importer.importerFact import ImporterFact

rs = FileDataDao().getStatusCount()
dictImportStatus = {'labels':[], 'parents':[], 'values':[], 'ids':[]}

for row in rs:
    dictImportStatus['labels'].append(row.label)
    dictImportStatus['parents'].append(row.parent)
    dictImportStatus['values'].append(row.values_)
    dictImportStatus['ids'].append(row.id)
    
dictImportStatus['ids'] = dictImportStatus['labels'] + dictImportStatus['ids'] 
dictImportStatus['ids'] = list(dict.fromkeys(dictImportStatus['ids']))
# dictImportStatus['labels'] = list(dict.fromkeys(dictImportStatus['labels']))
dictImportStatus['ids'] = [ x for x in dictImportStatus['ids'] if x is not None ]

listFileStatus = []
for fileStatus in [ x for x in list(dict.fromkeys(dictImportStatus['parents'])) if x is not None ]:
    listFileStatus.append({'label': fileStatus, 'value': fileStatus})

ddImportStatus = dcc.Dropdown(
    id='dd-fileStatus',
    options=listFileStatus,
    value=None,
    clearable=False
)

listFactStatus = []
for factStatus in [ x for x in list(dict.fromkeys(dictImportStatus['labels'])) if x is not None ]:
    listFactStatus.append({'label': factStatus, 'value': factStatus})

ddstatus = dcc.Dropdown(
    id='dd-factStatus',
    options=listFactStatus,
    value=None
)

sunburstImportStatus = go.Figure(go.Sunburst(
                            labels=dictImportStatus['labels'],
                            parents=dictImportStatus['parents'],
                            ids=dictImportStatus['ids'],
                            marker=dict(
                                colors=dictImportStatus['values'],
                                colorscale='RdBu'),
                            hovertemplate='%{color:.0f}'
                                ))

sunburstImportStatus.update_layout(margin=dict(t=0, l=0, r=0, b=0))

layout = html.Div([
                dcc.Graph(
                    id='graph',
                    figure=sunburstImportStatus,
                    style={'width': '70%', 'display': 'inline-block', 'float': 'left'}),
                html.Div(id='fig-fileStatus', style={'clear': 'both'}),
                html.Label(["Import File Status", ddImportStatus]),
                html.Label(["Import Fact Status", ddstatus]),
                html.Button(id='btn-submit-processFile', n_clicks=0, children='Submit'),
                html.Div(id='hidden2-div', style={'display':'none'})
                ])


@app.callback(
    Output('graph', "figure"),
    [Input('btn-submit-processFile', 'n_clicks')],
    [State('dd-fileStatus', 'value'),
     State('dd-factStatus', 'value')])
def doSubmitProcessFile(n_clicks, fileStatus, factStatus):
    print("entro")
    print(fileStatus, factStatus)
    if (n_clicks > 0):
        logging.info("START")
        createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
        session = DBConnector().getNewSession()
        if (factStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr='fileStatus', statusValue=fileStatus,statusAttr2='status',statusValue2=factStatus,session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFact)
            importerExecutor.execute(fileDataList)
            print(fileDataList)
        elif (fileStatus is not None):    
            print(fileStatus, factStatus)
            fileDataList = FileDataDao().getFileData2(statusAttr='fileStatus', statusValue=fileStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFile)
            importerExecutor.execute(fileDataList)
        
#         

    
    rs = FileDataDao().getStatusCount()
    dictImportStatus = {'labels':[], 'parents':[], 'values':[], 'ids':[]}
    
    for row in rs:
        dictImportStatus['labels'].append(row.label)
        dictImportStatus['parents'].append(row.parent)
        dictImportStatus['values'].append(row.values_)
        dictImportStatus['ids'].append(row.id)
        
    dictImportStatus['ids'] = dictImportStatus['labels'] + dictImportStatus['ids'] 
    dictImportStatus['ids'] = list(dict.fromkeys(dictImportStatus['ids']))
    # dictImportStatus['labels'] = list(dict.fromkeys(dictImportStatus['labels']))
    dictImportStatus['ids'] = [ x for x in dictImportStatus['ids'] if x is not None ]
    
    print(dictImportStatus)
    
    sunburstImportStatus = go.Figure(go.Sunburst(
                                labels=dictImportStatus['labels'],
                                parents=dictImportStatus['parents'],
                                ids=dictImportStatus['ids'],
                                marker=dict(
                                    colors=dictImportStatus['values'],
                                    colorscale='RdBu'),
                                hovertemplate='%{color:.0f}'
                                    ))
    
    sunburstImportStatus.update_layout(margin=dict(t=0, l=0, r=0, b=0))
    return sunburstImportStatus
    
    

