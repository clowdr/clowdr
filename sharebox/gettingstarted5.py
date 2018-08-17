import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import numpy as np
import plotly

app = dash.Dash()

app.scripts.config.serve_locally = True
# app.css.config.serve_locally = True

DF_GAPMINDER = pd.read_csv(
    'https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv'
)
DF_GAPMINDER = DF_GAPMINDER[DF_GAPMINDER['year'] == 2007]
DF_GAPMINDER.loc[0:20]

with open('./sharebox/clowdr-summary.json') as usage_fhandle:
    experiment_dict = json.load(usage_fhandle)

table_dict = []
for exp in experiment_dict:
    table_dict += [{'Task': exp['Task ID']}]
table_dict 
#plotting_dict() = 1
#param_dict() = 1

print(experiment_dict[0].keys())

#import pdb;pdb.set_trace()
#table_columns = ['Task ID', 'Tool Name', 'Time: Total (s)']

app.layout = html.Div([
    html.H4('Gapminder DataTable'),
    dt.DataTable(
        # rows=DF_GAPMINDER.to_dict('records'),
        rows=table_dict,

        # optional - sets the order of columns
        # columns=sorted(DF_GAPMINDER.columns),
        columns=list(table_dict[0].keys()),

        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-gapminder'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-gapminder'
    ),
], className="container")


@app.callback(
    Output('datatable-gapminder', 'selected_row_indices'),
    [Input('graph-gapminder', 'clickData'),
     Input('datatable-gapminder', 'rows')],
    [State('datatable-gapminder', 'selected_row_indices')])
def update_selected_row_indices(clickData, rows, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            curve = point['curveNumber'] // 3
            if curve in selected_row_indices:
                selected_row_indices.remove(curve)
            else:
                selected_row_indices.append(curve)
    return selected_row_indices


@app.callback(
    Output('graph-gapminder', 'figure'),
    [Input('datatable-gapminder', 'rows'),
     Input('datatable-gapminder', 'selected_row_indices')])
def update_figure(rows, selected_row_indices):
    rows = [exp
            for row in rows
            for exp in experiment_dict
            if row['Task'] == exp['Task ID']]
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('Memory Profile', 'Time Profile', 'Memory Profile',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(rows)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    for i, row in enumerate(rows):
        fig.append_trace({
            'x': row['Time: Series (s)'],
            'y': row['RAM: Series (MB)'],
            'mode': 'lines+markers',
            'line': {'color': marker['color'][i]},
            'customdata': (row, i),
            'opacity': 0.6,
            'name': 'Task {}'.format(row['Task ID'])
        }, 1, 1)
        fig.append_trace({
            'x': row['Time: Series (s)'],
            'y': row['Time: Series (s)'],
            'mode': 'lines+markers',
            'line': {'color': marker['color'][i]},
            'customdata': (row, i),
            'opacity': 0.8,
            'name': 'Task {}'.format(row['Task ID'])
        }, 2, 1)
        fig.append_trace({
            'x': row['Time: Series (s)'],
            'y': row['RAM: Series (MB)'],
            'mode': 'lines',
            'line': {'color': marker['color'][i]},
            'customdata': (row, i),
            'opacity': 0.8,
            'name': 'Task {}'.format(row['Task ID'])
        }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)

