import logging
import math

from dash.dependencies import Input, Output, State
from pandas.core.frame import DataFrame
from plotly.graph_objs import Layout

from base.dbConnector import DBConnector
from dao.fileDataDao import FileDataDao
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dataImport.importerExecutor import ImporterExecutor
from importer.importerCalculate import ImporterCalculate
from importer.importerCompany import ImporterCompany
from importer.importerCopy import ImporterCopy
from importer.importerEntityFact import ImporterEntityFact
from importer.importerExpression import ImporterExpression
from importer.importerFact import ImporterFact
from importer.importerFile import ImporterFile
from importer.importerPrice import ImporterPrice
import pandas as pd
import plotly.graph_objects as go
from tools.tools import createLog
from valueobject.constant import Constant
from valueobject.constantStatus import ConstantStatus
from web.app import app


hoverTemplate = '<b>%{customdata} </b> <br>%{label}<br>%{text} '
levels2 = [ConstantStatus.EXPRESSION_STATUS, ConstantStatus.CALCULATE_STATUS, ConstantStatus.COPY_STATUS, ConstantStatus.FACT_STATUS]  # levels used for the hierarchical chart
value_column = 'value_'

ddFileStatus = dcc.Dropdown(
    id='dd-fileStatus',
    value=None,
    clearable=False
)

ddCompanyStatus = dcc.Dropdown(
    id='dd-companyStatus',
    value=None,
)

ddListedStatus = dcc.Dropdown(
    id='dd-listedStatus',
    value=None,
)

ddEntityStatus = dcc.Dropdown(
    id='dd-entityStatus',
    value=None
)

ddPriceStatus = dcc.Dropdown(
    id='dd-priceStatus',
    value=None
)

ddFactStatus = dcc.Dropdown(
    id='dd-factStatus',
    value=None
)

ddCopyStatus = dcc.Dropdown(
    id='dd-copyStatus',
    value=None,
    clearable=False
)

ddCalculateStatus = dcc.Dropdown(
    id='dd-calculateStatus',
    value=None,
    clearable=False
)

ddExpressionStatus = dcc.Dropdown(
    id='dd-expressionStatus',
    value=None,
    clearable=False
)

layout2 = Layout(
    #paper_bgcolor='#002B36'
)

layout = dbc.Container(
            [
                dbc.Row([
                    dbc.Col([
                        dcc.Graph(
                            id='graph',
                            figure=go.Figure(data=go.Sunburst(), layout=layout2),
                            style={'width': '100%'}),
                        html.Div(id='fig-status1')
                    ]),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody([
                                dbc.Row(html.Label(["Import File Status", ddFileStatus])),
                                dbc.Row(html.Label(["Import Company Status", ddCompanyStatus])),
                                dbc.Row(html.Label(["Import Listed Status", ddListedStatus])),
                                dbc.Row(html.Label(["Import Entity Status", ddEntityStatus])),
                                dbc.Row(html.Label(["Import Price Status", ddPriceStatus])),
                                dbc.Row(html.Label(["Import Fact Status", ddFactStatus])),
                                dbc.Row(dbc.Button(id='btn-submit-processStatus', n_clicks=0, children='Submit'))
                            ]),
                        body=True),
                    width=3),
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Graph(
                            id='graph2',
                            figure=go.Figure(go.Sunburst()),
                            style={'width': '100%'}),
                        html.Div(id='fig-status2'),
                    ]),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody([
                                dbc.Row(html.Label(["Import Copy Status", ddCopyStatus])),
                                dbc.Row(html.Label(["Import Calculate Status", ddCalculateStatus])),
                                dbc.Row(html.Label(["Import Expression Status", ddExpressionStatus])),
                                dbc.Row(dbc.Button(id='btn-submit-processStatus2', n_clicks=0, children='Submit'))]
                            ),
                        body=True),
                        width=3
                    )
                ])    
            ]
        )

@app.callback(
    [Output('graph', "figure"),
     Output('dd-fileStatus', "options"),
     Output('dd-companyStatus', "options"),
     Output('dd-listedStatus', "options"),
     Output('dd-entityStatus', "options"),
     Output('dd-priceStatus', "options"),
     Output('dd-factStatus', "options")],
    [Input('btn-submit-processStatus', 'n_clicks')],
    [State('dd-fileStatus', 'value'),
     State('dd-companyStatus', 'value'),
     State('dd-listedStatus', 'value'),
     State('dd-entityStatus', 'value'),
     State('dd-priceStatus', 'value'),
     State('dd-factStatus', 'value')])
def doSubmitProcessStatus1(n_clicks, fileStatus, companyStatus, listedStatus, entityStatus, priceStatus, factStatus):
    if (n_clicks > 0):
        logging.info("START")
        print("Start")  
        createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
        session = DBConnector().getNewSession()
        
        if (factStatus is not None):
            fileDataList = FileDataDao().getFileData4(statusAttr=ConstantStatus.PRICE_STATUS, statusValue=priceStatus, statusAttr2=ConstantStatus.FACT_STATUS, statusValue2=factStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFact)
            importerExecutor.execute(fileDataList)
        elif (priceStatus is not None):
            fileDataList = FileDataDao().getFileData4(statusAttr=ConstantStatus.ENTITY_FACT_STATUS, statusValue=entityStatus, statusAttr2=ConstantStatus.PRICE_STATUS, statusValue2=priceStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=False, importerClass=ImporterPrice)
            importerExecutor.execute(fileDataList)
        elif (entityStatus is not None):
            if(listedStatus == 'LISTED'): 
                listed = 1 
            else: 
                listed = 0
            fileDataList = FileDataDao().getFileData3(statusAttr=ConstantStatus.COMPANY_STATUS, statusValue=companyStatus, statusAttr2=ConstantStatus.ENTITY_FACT_STATUS, statusValue2=entityStatus, session=session, listed=listed)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterEntityFact)
            importerExecutor.execute(fileDataList)
        elif (companyStatus is not None):
            fileDataList = FileDataDao().getFileData4(statusAttr=ConstantStatus.FILE_STATUS, statusValue=fileStatus, statusAttr2=ConstantStatus.COMPANY_STATUS, statusValue2=companyStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterCompany)
            importerExecutor.execute(fileDataList)
        elif (fileStatus is not None):    
            fileDataList = FileDataDao().getFileData6(statusAttr=ConstantStatus.FILE_STATUS, statusValue=fileStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterFile)
            importerExecutor.execute(fileDataList)

    rs = FileDataDao().getStatusCount2()
    df = DataFrame(rs, columns=[ConstantStatus.FILE_STATUS, ConstantStatus.COMPANY_STATUS, 'listedStatus', ConstantStatus.ENTITY_FACT_STATUS, ConstantStatus.PRICE_STATUS, ConstantStatus.FACT_STATUS, 'value_'])
    levels = [ConstantStatus.FACT_STATUS, ConstantStatus.PRICE_STATUS, ConstantStatus.ENTITY_FACT_STATUS, 'listedStatus', ConstantStatus.COMPANY_STATUS, ConstantStatus.FILE_STATUS]
    df_all_trees = build_hierarchical_dataframe(df, levels, value_column)
    sunburstImportStatus = go.Figure(go.Sunburst(
                                labels=df_all_trees['label'],
                                parents=df_all_trees['parent'],
                                ids=df_all_trees['id'],
                                customdata=df_all_trees['level'],
                                text = df_all_trees['value'],
                                name="",
                                marker=dict(
                                    colors=df_all_trees['color'],
                                    colorscale='RdBu'),
                                hovertemplate=hoverTemplate
                                    ), layout=layout2)
    
    sunburstImportStatus.update_layout(margin=dict(t=0, l=0, r=0, b=0))
    
    listFileStatus = getUniqueValues(df_all_trees, ConstantStatus.FILE_STATUS)
    listCompanyStatus = getUniqueValues(df_all_trees, ConstantStatus.COMPANY_STATUS)
    listListedStatus = getUniqueValues(df_all_trees, 'listedStatus')
    listEntityStatus = getUniqueValues(df_all_trees, ConstantStatus.ENTITY_FACT_STATUS)
    listPriceStatus = getUniqueValues(df_all_trees, ConstantStatus.PRICE_STATUS)
    listFactStatus = getUniqueValues(df_all_trees, ConstantStatus.FACT_STATUS)
    
    return sunburstImportStatus, listFileStatus, listCompanyStatus, listListedStatus, listEntityStatus, listPriceStatus, listFactStatus


@app.callback(
    [Output('graph2', "figure"),
     Output('dd-copyStatus', "options"),
     Output('dd-calculateStatus', "options"),
     Output('dd-expressionStatus', "options")],
    [Input('btn-submit-processStatus2', 'n_clicks')],
    [State('dd-copyStatus', 'value'),
     State('dd-calculateStatus', 'value'),
     State('dd-expressionStatus', 'value')])
def doSubmitProcessStatus2(n_clicks, copyStatus, calculateStatus, expressionStatus):
    if (n_clicks > 0):
        logging.info("START")
        print("Start")  
        createLog(Constant.LOGGER_IMPORT_GENERAL, logging.DEBUG)
        session = DBConnector().getNewSession()
        if (expressionStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr=ConstantStatus.EXPRESSION_STATUS, statusValue=expressionStatus, statusAttr2=ConstantStatus.CALCULATE_STATUS, statusValue2=calculateStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=False, importerClass=ImporterExpression)
            importerExecutor.execute(fileDataList)
        elif (calculateStatus is not None):
            fileDataList = FileDataDao().getFileData4(statusAttr=ConstantStatus.CALCULATE_STATUS, statusValue=calculateStatus, statusAttr2=ConstantStatus.COPY_STATUS, statusValue2=copyStatus, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterCalculate)
            importerExecutor.execute(fileDataList)        
        elif (copyStatus is not None):
            fileDataList = FileDataDao().getFileData3(statusAttr=ConstantStatus.COPY_STATUS, statusValue=copyStatus, statusAttr2=ConstantStatus.FACT_STATUS, statusValue2=Constant.STATUS_OK, session=session)
            importerExecutor = ImporterExecutor(threadNumber=4, maxProcessInQueue=5, replace=False, isSequential=True, importerClass=ImporterCopy)
            importerExecutor.execute(fileDataList)
    
    rs = FileDataDao().getStatusCount3()
    df = DataFrame(rs, columns=[ConstantStatus.FACT_STATUS, ConstantStatus.COPY_STATUS, ConstantStatus.CALCULATE_STATUS, ConstantStatus.EXPRESSION_STATUS, 'value_'])
    df_all_trees = build_hierarchical_dataframe(df, levels2, value_column)
    s = go.Sunburst(
            labels=df_all_trees['label'],
            parents=df_all_trees['parent'],
            ids=df_all_trees['id'],
            customdata= df_all_trees['level'],
            text = df_all_trees['value'],
            name="",
            marker=dict(
                colors=df_all_trees['color'],
                colorscale='RdBu'),
            hovertemplate=hoverTemplate
                )
    sunburstStatus2 = go.Figure(s, layout=layout2)
    sunburstStatus2.update_layout(margin=dict(t=0, l=0, r=0, b=0))
    
    listCopyStatus = getUniqueValues(df_all_trees, ConstantStatus.COPY_STATUS)
    listCalculateStatus = getUniqueValues(df_all_trees, ConstantStatus.CALCULATE_STATUS)
    listExpressionStatus = getUniqueValues(df_all_trees, ConstantStatus.EXPRESSION_STATUS)
    
    return sunburstStatus2, listCopyStatus, listCalculateStatus, listExpressionStatus


def getUniqueValues(df_all_trees, key):
    resultList = []
    for item in [ x for x in list(dict.fromkeys(df_all_trees[key])) if isinstance(x, str)]:
        resultList.append({'label': item, 'value': item})
    return resultList

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
            #set total series
            df_tree['id'] = dfg[level].copy()
            df_tree['parent'] = 'total'
            df_tree['label'] = dfg[level].copy()
            df_tree[level] = dfg[level].copy()
            df_tree['level'] = level
        df_tree['value'] = dfg[value_column]
        df_tree['color'] = getColorCode(df_tree['label'])
        df_all_trees = df_all_trees.append(df_tree, ignore_index=True)
    total = pd.Series(dict(id='total', label='total', parent='', value=df[value_column].sum(), level=''))
    df_all_trees = df_all_trees.append(total, ignore_index=True)
    return df_all_trees

def getColorCode(labels):
    returnList = []
    for item in labels.iteritems(): 
        if (item[1] == 'OK' or item[1] == 'LISTED'):
            returnList.append('rgb(0,102,51)')
        elif (item[1] == 'NO_DATA'):
            returnList.append('rgb(204,204,0)')
        elif (item[1] == 'ERROR' or item[1] == 'FNF' or item[1] == 'XML_FNF'):
            returnList.append('rgb(153,0,0)')
        elif (item[1] == 'INIT'):
            returnList.append('rgb(0,76,153)')
        elif (item[1] == 'PENDING'):
            returnList.append('rgb(204,255,204)')
        else:
            returnList.append('white')
    return pd.Series(returnList)
