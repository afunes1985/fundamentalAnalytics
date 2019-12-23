import logging
import math

from dash.dependencies import Input, Output, State
from pandas.core.frame import DataFrame

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
import dash_core_components as dcc
import dash_html_components as html
from dataImport.importerExecutor import ImporterExecutor
from importer.importerFact import ImporterFact
from importer.importerFile import ImporterFile
import pandas as pd
import plotly.graph_objects as go
from tools.tools import createLog
from valueobject.constant import Constant
from web.app import app
from importer.importerEntityFact import ImporterEntityFact
from importer.importerPrice import ImporterPrice

levels = ['priceStatus', 'entityStatus', 'status', 'fileStatus']  # levels used for the hierarchical chart
value_column = 'value_'

ddFileStatus = dcc.Dropdown(
    id='dd-fileStatus',
    value=None,
    clearable=False
)

ddFactStatus = dcc.Dropdown(
    id='dd-factStatus',
    value=None
)

ddEntityStatus = dcc.Dropdown(
    id='dd-entityStatus',
    value=None
)

ddPriceStatus = dcc.Dropdown(
    id='dd-priceStatus',
    value=None
)

layout = html.Div([
                dcc.Graph(
                    id='graph',
                    figure=go.Figure(go.Sunburst()),
                    style={'width': '70%', 'display': 'inline-block', 'float': 'left'}),
                html.Div(id='fig-fileStatus', style={'clear': 'both'}),
                html.Label(["Import File Status", ddFileStatus]),
                html.Label(["Import Fact Status", ddFactStatus]),
                html.Label(["Import Entity Status", ddEntityStatus]),
                html.Label(["Import Price Status", ddPriceStatus]),
                html.Button(id='btn-submit-processFile', n_clicks=0, children='Submit'),
                html.Div(id='hidden2-div', style={'display':'none'})
                ])


@app.callback(
    [Output('graph', "figure"),
     Output('dd-fileStatus', "options"),
     Output('dd-factStatus', "options"),
     Output('dd-entityStatus', "options"),
     Output('dd-priceStatus', "options")],
    [Input('btn-submit-processFile', 'n_clicks')],
    [State('dd-fileStatus', 'value'),
     State('dd-factStatus', 'value'),
     State('dd-entityStatus', 'value'),
     State('dd-priceStatus', 'value')])
def doSubmitProcessFile(n_clicks, fileStatus, factStatus, entityStatus, priceStatus):
    if (n_clicks > 0):
        logging.info("START")
        print("Start")  
        createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
        session = DBConnector().getNewSession()
        
        if (priceStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr='entityStatus', statusValue=entityStatus, statusAttr2='priceStatus', statusValue2=priceStatus, session=session, errorMessage='%timed out%')
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=False, importerClass=ImporterPrice)
            importerExecutor.execute(fileDataList)
        elif (entityStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr='status', statusValue=factStatus, statusAttr2='entityStatus', statusValue2=entityStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterEntityFact)
            importerExecutor.execute(fileDataList)
        elif (factStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr='fileStatus', statusValue=fileStatus, statusAttr2='status', statusValue2=factStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFact)
            importerExecutor.execute(fileDataList)
        elif (fileStatus is not None):    
            fileDataList = FileDataDao().getFileData2(statusAttr='fileStatus', statusValue=fileStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFile)
            importerExecutor.execute(fileDataList)

    rs = FileDataDao().getStatusCount2()
    df = DataFrame(rs, columns=['fileStatus', 'status', 'entityStatus', 'priceStatus', 'value_'])
    df_all_trees = build_hierarchical_dataframe(df, levels, value_column)
    sunburstImportStatus = go.Figure(go.Sunburst(
                                labels=df_all_trees['label'],
                                parents=df_all_trees['parent'],
                                ids=df_all_trees['id'],
                                customdata = df_all_trees['level'],
                                name = "",
                                marker=dict(
                                    colors=df_all_trees['value'],
                                    colorscale='RdBu'),
                                hovertemplate='<b>%{customdata} </b> <br> %{color:.0f}'
                                    ))
    
    sunburstImportStatus.update_layout(margin=dict(t=0, l=0, r=0, b=0))
    
    listFileStatus = []
    for fileStatus in [ x for x in list(dict.fromkeys(df_all_trees['fileStatus'])) if isinstance(x, str)]:
        listFileStatus.append({'label': fileStatus, 'value': fileStatus})
        
    listFactStatus = []
    for factStatus in [ x for x in list(dict.fromkeys(df_all_trees['status'])) if isinstance(x, str)]:
        listFactStatus.append({'label': factStatus, 'value': factStatus})

    listEntityStatus = []
    for entityStatus in [ x for x in list(dict.fromkeys(df_all_trees['entityStatus'])) if isinstance(x, str)]:
        listEntityStatus.append({'label': entityStatus, 'value': entityStatus})
        
    listPriceStatus = []
    for priceStatus in [ x for x in list(dict.fromkeys(df_all_trees['priceStatus'])) if isinstance(x, str)]:
        listPriceStatus.append({'label': priceStatus, 'value': priceStatus})
    
    return sunburstImportStatus, listFileStatus, listFactStatus, listEntityStatus, listPriceStatus


def build_hierarchical_dataframe(df, levels, value_column):
    """
    Build a hierarchy of levels for Sunburst or Treemap charts.

    Levels are given starting from the bottom to the top of the hierarchy, 
    ie the last level corresponds to the root.
    """
    c = ['id', 'parent', 'value', 'level'] + levels
    
    df_all_trees = pd.DataFrame(columns=c)
    for i, level in enumerate(levels):
        df_tree = pd.DataFrame(columns=['id', 'parent', 'value', 'level'])
        dfg = df.groupby(levels[i:]).sum(numerical_only=True)
        dfg = dfg.reset_index()
        if i < len(levels) - 1:
            for t in range(i, len(levels)):
                if (t == i): 
                    df_tree['id'] = dfg[levels[t]].copy()
                    df_tree['parent'] = dfg[levels[t + 1]].copy()
                else:
                    df_tree['id'] = dfg[levels[t]].copy() + " - " + df_tree['id']
                    if(t + 1 < len(levels)):
                        df_tree['parent'] = dfg[levels[t + 1]].copy() + " - " + df_tree['parent']
            df_tree['label'] = dfg[level].copy()
            df_tree[level] = dfg[level].copy()
            df_tree['level'] = level
        else:
            df_tree['id'] = dfg[level].copy()
            df_tree['parent'] = 'total'
            df_tree['label'] = dfg[level].copy()
            df_tree[level] = dfg[level].copy()
            df_tree['level'] = level
        df_tree['value'] = dfg[value_column]
        df_all_trees = df_all_trees.append(df_tree, ignore_index=True)
    total = pd.Series(dict(id='total', label = 'total', parent='',value=df[value_column].sum(), level = ''))
    df_all_trees = df_all_trees.append(total, ignore_index=True)
    return df_all_trees

