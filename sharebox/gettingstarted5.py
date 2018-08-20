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

with open('./sharebox/clowdr-summary.json') as usage_fhandle:
    experiment_dict = json.load(usage_fhandle)

task_dict = []
invo_dict= []
for exp in experiment_dict:
    task_dict += [{'Task': exp['Task ID'],
                   'Tool': exp['Tool Name'],
                   'Duration (s)': exp['Time: Total (s)'],
                   'Max RAM (MB)': exp['RAM: Max (MB)'],
                   'Exit Code': exp['Exit Code']}]
    tmp_dict = {'Task' : exp['Task ID']}
    tmp_dict.update({key.replace('Param: ', ''): exp[key]
                     for key in exp.keys()
                     if key.startswith('Param: ')})
    invo_dict += [tmp_dict]

print(invo_dict[0].keys())
print(experiment_dict[0].keys())

app.layout = html.Div([
    html.H4('Clowdr Experiment Explorer'),
    dcc.Tabs(id='tabs',
             value='stats-tab',
             children=[
        dcc.Tab(label="Statistics",
                value='stats-tab',
                children=[
                    dt.DataTable(
                        rows=task_dict,
                        # optional - sets the order of columns
                        columns=list(task_dict[0].keys()),

                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='datatable-clowdrexp')
                ]),
        dcc.Tab(label="Invocations",
                value='invo-tab',
                children=[
                    dt.DataTable(
                        rows=invo_dict,
                        # optional - sets the order of columns
                        columns=list(invo_dict[0].keys()),

                        row_selectable=True,
                        filterable=True,
                        sortable=True,
                        editable=False,
                        selected_row_indices=[],
                        id='invotable-clowdrexp')
                ])
    ]),
    dcc.Graph(
        id='graph-clowdrexp'
    ),
], className="container")


@app.callback(
    Output('invotable-clowdrexp', 'selected_row_indices'),
    [Input('graph-clowdrexp', 'clickData'),
     Input('datatable-clowdrexp', 'rows'),
     Input('tabs', 'value')],
    [State('datatable-clowdrexp', 'selected_row_indices'),
     State('invotable-clowdrexp', 'selected_row_indices')])
def update_selected_row_indices(clickData, rows, tab, data_rows, invo_rows):
    if clickData and tab == 'invo-tab':
        for point in clickData['points']:
            curve = point['curveNumber'] // 3  # TODO: replace with # of graphs
            if curve in data_rows:
                invo_rows.remove(curve)
            else:
                invo_rows.append(curve)
    elif tab == 'invo-tab':
        return data_rows
    else:
        return invo_rows


@app.callback(
    Output('datatable-clowdrexp', 'selected_row_indices'),
    [Input('graph-clowdrexp', 'clickData'),
     Input('datatable-clowdrexp', 'rows'),
     Input('tabs', 'value')],
    [State('datatable-clowdrexp', 'selected_row_indices'),
     State('invotable-clowdrexp', 'selected_row_indices')])
def update_selected_row_indices(clickData, rows, tab, data_rows, invo_rows):
    if clickData and tab == 'stats-tab':
        for point in clickData['points']:
            curve = point['curveNumber'] // 3  # TODO: replace with # of graphs
            if curve in data_rows:
                data_rows.remove(curve)
            else:
                data_rows.append(curve)
    elif tab == 'stats-tab':
        return invo_rows
    else:
        return data_rows


@app.callback(
    Output('graph-clowdrexp', 'figure'),
    [Input('datatable-clowdrexp', 'rows'),
     Input('datatable-clowdrexp', 'selected_row_indices'),
     Input('invotable-clowdrexp', 'selected_row_indices')],
    [State('tabs', 'value')])
def update_figure(rows, data_rows, invo_rows, tab):
    if tab == 'stats-tab':
        indices = data_rows
    elif tab == 'invo-tab':
        indices = invo_rows
    rows = [exp
            for row in rows
            for exp in experiment_dict
            if row['Task'] == exp['Task ID']]
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('Memory Profile', 'Time Profile', 'Memory Profile',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(rows)}
    for i in (indices or []):
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

