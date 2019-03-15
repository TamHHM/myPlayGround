import pandas as pd
import numpy as np
import plotly.plotly as py
import plotly.graph_objs as go
import plotly
from plotly import tools
from datetime import datetime
from collections import Counter

import itertools
import logging, logging.config, loggerSetup
import sys

import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table
from dash.dependencies import Input, Output, State
from IPython import display
import json
import boto3
import os

def show_app(app, port = 9999,
             width = 700,
             height = 350,
             offline = False,
            in_binder = None):
    in_binder ='JUPYTERHUB_SERVICE_PREFIX' in os.environ if in_binder is None else in_binder
    if in_binder:
        base_prefix = '{}proxy/{}/'.format(os.environ['JUPYTERHUB_SERVICE_PREFIX'], port)
        url = 'https://hub.mybinder.org{}'.format(base_prefix)
        app.config.requests_pathname_prefix = base_prefix
    else:
        url = 'http://localhost:%d' % port

    iframe = '<a href="{url}" target="_new">Open in new window</a><hr><iframe src="{url}" width={width} height={height}></iframe>'.format(url = url,
                                                                                  width = width,
                                                                                  height = height)

    display.display_html(iframe, raw = True)
    if offline:
        app.css.config.serve_locally = True
        app.scripts.config.serve_locally = True
    return app.run_server(debug=False, # needs to be false in Jupyter
                          host = '0.0.0.0',
                          port=port,
			              threaded=False,
                          processes=6)

# plotly.offline.init_notebook_mode(connected=True)

# Starting logger
loggerSetup.configure()
logger = logging.getLogger(__name__)

# get from AWS
# filename = os.getenv('AWS_FILE')
# if filename not in os.listdir('./tmp/'):
#     print('Downloading data from S3')
#     # boto3.set_stream_logger('boto3.resources', logging.DEBUG)
#     client=boto3.client('s3',aws_access_key_id=os.getenv('AWS_AKEY'),aws_secret_access_key=os.getenv('AWS_SKEY'), region_name='ap-southeast-2')
#     # client=boto3.client('s3',aws_access_key_id=os.getenv('AWS_AKEY'),aws_secret_access_key=os.getenv('AWS_SKEY'))
#     # client.download_file("aus-monash", os.getenv('AWS_FILE'), "dashboard.csv")
#     client.download_file("aus-monash", filename, "./tmp/"+filename)
#
# tabularData = pd.read_csv('./tmp/'+filename)

def main():
    filename = os.getenv('AWS_FILE')
    location = '/tmp/'
    if filename not in os.listdir(location):
        print('Downloading data from S3')
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_AKEY'),
            aws_secret_access_key=os.getenv('AWS_SKEY'),
            region_name='eu-central-1',
        )
        resource = session.resource('s3')
        my_bucket = resource.Bucket(os.getenv('AWS_BUCKET'))
        my_bucket.download_file(filename, location + filename)

        # client=boto3.client('s3',aws_access_key_id=os.getenv('AWS_AKEY'),aws_secret_access_key=os.getenv('AWS_SKEY'),
        #                         region_name='eu-central-1')
        # client.download_file("aus-monash-frankfurt", filename, location + filename)

    print('Finish downloading.')

    tabularData = pd.read_csv(location + filename)
    columns = tabularData.columns.drop(['const', 'stayDuration', 'interTripDays'])
    tabularData[columns] = tabularData[columns].astype(str)
    tabularData['stayDuration_total'] = tabularData.stayDuration
    tabularData['stayDuration_avg'] = tabularData.stayDuration
    tabularData['interTripDays_total'] = tabularData.interTripDays
    tabularData['interTripDays_avg'] = tabularData.interTripDays
    tabularData = tabularData.rename(columns = {'const': 'Admission'})

    filterList = ['eventType', 'sex', 'postcode', 'eventType', 'admissionsource',
                  'separationmode', 'transferdestination', 'transfersource', 'caretype',
                  'criterionforadmission', 'intentiontoreadmit', 'proc01', 'pph_je',
                  'pph_je_type', 'age_cat3', 'pph_cd',
                  'yearmon', 'year', 'CardiovascularRelated', 'postcode_dc', 'campusName',
                  'primarydxName', 'Cardiovascular', 'seifa_quantile']
    sortList = ['Admission', 'stayDuration_total', 'stayDuration_avg', 'interTripDays_total', 'interTripDays_avg']

    print('Spinning up the dashboard...')

    ### HTML config #########################################################################
    app = dash.Dash()

    splitRow = \
        html.Div([
            html.Div(html.H6(children='Split on'), className="two columns"),

            html.Div([
                dcc.Dropdown(
                    id = 'filter_dropbox',
                    options=[{'label': item, 'value': item}\
                             for item in filterList],
                    value=filterList[0],
                ),
            ], className="three columns"),

            html.Div([html.Button('Add', id='addButton')], className="two columns"),

            html.Div(html.H6(children='Sort on'), className="two columns"),

            html.Div([
                dcc.Dropdown(
                    id = 'sort_dropbox',
                    options=[{'label': item, 'value': item}\
                             for item in sortList],
                    value=sortList[0],
                ),
            ], className="three columns"),
        ], style={'textAlign': 'center',
                 'font-family':'Courier New, monospace',
                 'color':'#3a3a3a'},
           className="row")

    drawRow = \
        html.Div([
            html.Div(html.H6(children='Combination'), className="two columns"),
            html.Div(dcc.Input(id='input_box', type='text', size=60, value=''), className="six columns"),
            html.Div([html.Button('Draw', id='drawButton')], className="two columns"),
            html.Div([html.Button('Clear', id='clearButton')], className="two columns"),

        ], style={'textAlign': 'center',
                 'font-family':'Courier New, monospace',
                 'color':'#3a3a3a'},
           className="row")

    tableRow = \
        dash_table.DataTable(
            id='table',
        )

    app.layout = \
        html.Div([
            splitRow,
            drawRow,
            tableRow,
            html.Div(id='intermediate', style={'display': 'none'})
        ])

    # app.scripts.config.serve_locally = True
    app.css.append_css({
        'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
    })

    ### Callback config#########################################################################

    @app.callback(
        Output('input_box', 'value'),
        [Input('addButton', 'n_clicks_timestamp'),
         Input('clearButton', 'n_clicks_timestamp'),
        ],
        [State('filter_dropbox', 'value'),
         State('input_box', 'value'),
        ])
    def updateInput(addTime, clearTime, filterVal, textVal):
        addTimeValid = addTime if not pd.isnull(addTime) else 0
        clearTimeValid = clearTime if not pd.isnull(clearTime) else 0
        textValValid = textVal if not pd.isnull(textVal) else ''
        if addTimeValid > clearTimeValid:
            print('Trigger add')
            return (textVal + ' | ' + filterVal if len(textVal)!=0 else filterVal)
        elif addTimeValid < clearTimeValid:
            print('Trigger clear')
            return ''
        else:
            print('Trigger nothing')
            return ''

    # Aggregate the admission based on the order of the filters
    # The data is successive filterred. In each filter, keep only the top 5 contributing item, the rest is
    # relabeled as Others.
    # The code is written in an unoptimized recursive way.
    def aggregationWithTop5(df, keyIdx, keys, sortOn):
        # get keys from global
        curKey = keys[keyIdx] # reference to current split
        nextKeys = keys[keyIdx+1:] # reference to the next splits
        inputData = pd.DataFrame.copy(df)

        # Some keys should be kept full
        keysFull = ['campusName', 'eventType', 'separationmode', 'year', 'yearmon']

        # Keep only the top 5, the rest becomes Others

        temp = inputData.groupby(curKey).agg({
            'Admission': pd.Series.sum,
            'stayDuration_total': pd.Series.sum,
            'stayDuration_avg': pd.Series.mean,
            'interTripDays_total': pd.Series.sum,
            'interTripDays_avg': pd.Series.mean,
        }).sort_values(by=sortOn, ascending=False)

        # Preparation to append the % after the categories
        if (curKey not in keysFull):
            maxItem = min(5, len(temp))
        else:
            maxItem = len(temp)

        mapPercent = round(100.0*temp[sortOn].iloc[:maxItem]/temp[sortOn].sum(), 2)
        mapPercent = pd.concat([mapPercent, pd.Series({'Others':100-mapPercent.sum()})])

        # Func to append %
        def appendPercentage(txt):
            return str(txt) + ' | ' + str(mapPercent.loc[txt]) + '%'

        # Append the % after the categories to top 5
        topIdx = temp.index[:maxItem]
        flagInTopFive = np.array([False]*len(inputData))
        for item in topIdx:
            flag = list(inputData[curKey]==item)
            flagInTopFive |= flag
            idx = inputData[flag].index
            inputData.loc[idx, curKey] = appendPercentage(item)

        # Replace item < top 5 to Others
        print('@@@ %s' % curKey)
        print(flagInTopFive)
        flagInTopFive = ~flagInTopFive
        if (curKey not in keysFull) & any(flagInTopFive):
            idx = inputData[flagInTopFive].index
            inputData.loc[idx, curKey] = appendPercentage('Others')
            topIdx = list(topIdx) + ['Others']

        newTopList = list(map(lambda x: appendPercentage(x), topIdx))
        print(newTopList)
        print(inputData[curKey].unique())
        if len(nextKeys) > 0:
            aggDf = pd.DataFrame()
            for key in newTopList: #inputData[curKey].unique():
                aggDf = pd.concat([aggDf, aggregationWithTop5(inputData[inputData[curKey]==key], keyIdx+1, keys, sortOn)])
            # aggDf = aggDf.rename(columns={'const':'Admission'})
            return aggDf
        else:
            inputData = inputData.groupby(keys,as_index=False).agg({
                'Admission': pd.Series.sum,
                'stayDuration_total': pd.Series.sum,
                'stayDuration_avg': pd.Series.mean,
                'interTripDays_total': pd.Series.sum,
                'interTripDays_avg': pd.Series.mean,
            })

            if (curKey == 'year') or (curKey == 'yearmon'):
                inputData[curKey] = inputData[curKey].astype(str)
            else:
                inputData = inputData.sort_values(by=sortOn, ascending=False)
            inputData[sortOn] = inputData[sortOn].astype(str) + ' | '\
                        + round(100.0*inputData[sortOn]/inputData[sortOn].sum(),2).astype(str) + '%'
            return inputData

    @app.callback(
        Output('intermediate', 'children'),
        [Input('drawButton', 'n_clicks')],
        [State('input_box', 'value'),
         State('sort_dropbox', 'value')
        ]
    )
    def doIntermediateTasks(click, value, sortVal):
        print(value)
        print(click)
        print(sortVal)
        print("I click draw.")
        if not pd.isnull(click):
            print('?1')
            clickValid = click
        else:
            print('?2')
            clickValid = 0

        print(clickValid)
        print('?!?!?!')
        if clickValid>0:
            print('Processing...')
            keys = value.split(' | ')
            aggData = aggregationWithTop5(tabularData, 0, keys, sortOn=sortVal)\
                            .reset_index().drop(columns='index').reset_index()
            # aggData = aggData.rename(columns={'const':'Admission'})
            # print(aggData)
            keysToClean = {}
            for i in range(len(keys)):
                key = keys[i]
                if i!=0:
                    prevkey = keys[i-1]
                    idx = aggData[aggData[key]==aggData[key].shift(1)][aggData[prevkey]==aggData[prevkey].shift(1)].index
                else:
                    idx = aggData[aggData[key]==aggData[key].shift(1)].index
                keysToClean[key] = idx
            for key in keys:
                aggData.loc[keysToClean[key], key] = ''
            print('Return Agg')
            # print(aggData)
            return aggData.to_json()
        else:
            print('Return nothing')
            return ''

    @app.callback(
        Output('table', 'columns'),
        [Input('intermediate', 'children')],
    )
    def updateTableCol(value):
        print("A0: %s." % str(type(value)))
        if not pd.isnull(value) and (value!=''):
            print("A1.")
            aggData = pd.read_json(value).sort_values('index')
            return [{"name": i, "id": i} for i in aggData.columns]
        else:
            return []


    @app.callback(
        Output('table', 'data'),
        [Input('intermediate', 'children')],
    )
    def updateTableRow(value):
        print("B0: %s." % str(type(value)))
        if not pd.isnull(value) and (value!=''):
            print("B1.")
            aggData = pd.read_json(value).sort_values('index')
            return aggData.to_dict("rows")
        else:
            return {}


    # @app.callback(
    #     Output('table', 'columns'),
    #     [Input('drawButton', 'n_clicks')],
    #     [State('input_box', 'value')]
    # )
    # def updateTableCol(click, value):
    #     print("1I click draw.")
    #     clickValid = click if not pd.isnull(click) else 0
    #     if clickValid>0:
    #         print("1I start drawing.")
    #         keys = value.split(' | ')
    #         aggData = aggregationWithTop5(tabularData, 0, keys)\
    #                         .reset_index().drop(columns='index').reset_index()
    #         aggData = aggData.rename(columns={'const':'Admission'})
    #         print(aggData)
    #         keysToClean = {}
    #         for i in range(len(keys)):
    #             key = keys[i]
    #             if i!=0:
    #                 prevkey = keys[i-1]
    #                 idx = aggData[aggData[key]==aggData[key].shift(1)][aggData[prevkey]==aggData[prevkey].shift(1)].index
    #             else:
    #                 idx = aggData[aggData[key]==aggData[key].shift(1)].index
    #             keysToClean[key] = idx
    #         for key in keys:
    #             aggData.loc[keysToClean[key], key] = ''
    #         return [{"name": i, "id": i} for i in aggData.columns]
    #     else:
    #         return None
    #
    #
    # @app.callback(
    #     Output('table', 'data'),
    #     [Input('drawButton', 'n_clicks')],
    #     [State('input_box', 'value')]
    # )
    # def updateTableRow(click, value):
    #     print("2I click draw.")
    #     clickValid = click if not pd.isnull(click) else 0
    #     if clickValid>0:
    #         print("2I start drawing.")
    #         keys = value.split(' | ')
    #         aggData = aggregationWithTop5(tabularData, 0, keys)\
    #                         .reset_index().drop(columns='index').reset_index()
    #         aggData = aggData.rename(columns={'const':'Admission'})
    #         keysToClean = {}
    #         for i in range(len(keys)):
    #             key = keys[i]
    #             if i!=0:
    #                 prevkey = keys[i-1]
    #                 idx = aggData[aggData[key]==aggData[key].shift(1)][aggData[prevkey]==aggData[prevkey].shift(1)].index
    #             else:
    #                 idx = aggData[aggData[key]==aggData[key].shift(1)].index
    #             keysToClean[key] = idx
    #         for key in keys:
    #             aggData.loc[keysToClean[key], key] = ''
    #         return aggData.to_dict("rows")
    #     else:
    #         return None

    ### Run dashboard
    show_app(app)


if __name__ == "__main__":
    main()
