#!/usr/bin/env python

from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import dash

import plotly.figure_factory as ff
import plotly

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

# Initialize Dash app
app = dash.Dash()
app.scripts.config.serve_locally = True


# Load and groom dataset
# -----> /start data grooming
# TODO: accept this as input
# TODO: start from a directory
# Load experiment data
with open('./sharebox/clowdr-summary.json') as usage_fhandle:
    experiment_dict = json.load(usage_fhandle)

# Intitialize two new data tables for visualization, resulting in:
#   - (Full) experiment table, to be used in visualizations
#   - Stats table, to be used for filtering based on summary info
#   - Invocation table, to be used for filtering based on invocation info
stat_dict = []
invo_dict = []
new_experiment_dict = []
for exp in experiment_dict:
    # Coerce task ID into integer value
    exp['Task ID'] = int(exp['Task ID'])
    time_s = datetime.strptime(exp['Time: Start'], "%Y-%m-%d %H:%M:%S")
    time_e = time_s + timedelta(0, exp['Time: Total (s)'])
    exp['Time: End'] = str(time_e)

    # Add relevant info to stats dict
    stat_dict += [{'Task': exp['Task ID'],
                   'Tool': exp['Tool Name'],
                   'Duration (s)': exp['Time: Total (s)'],
                   'Max RAM (MB)': exp['RAM: Max (MB)'],
                   'Exit Code': exp['Exit Code']}]

    # Add relevant info to invocation dict
    tmp_dict = {'Task' : exp['Task ID']}
    tmp_dict.update({key.replace('Param: ', ''): exp[key]
                     for key in exp.keys()
                     if key.startswith('Param: ')})
    invo_dict += [tmp_dict]

    # Save the minor updates back to the dictionary
    new_experiment_dict += [exp]

# Get rid of un-updated version of the dictionary
experiment_dict = new_experiment_dict

# Create look-up-table for mapping tasks to rows in the tables
idx_to_task_lut = {exp['Task ID']: idx
                   for idx, exp in enumerate(experiment_dict)}
task_to_idx_lut = {idx: exp['Task ID']
                   for idx, exp in enumerate(experiment_dict)}
# <------ /stop data grooming


# Create helper functions to be used by callbacks & page creation
# ------> /start helper function creation
# Creates the data table given the dictionary to be displayed and selected rows
def create_datatable(data_dict, selected_indices):
    return dt.DataTable(
                rows=data_dict,  # Table data
                columns=list(data_dict[0].keys()),  # Column names
                row_selectable=True,  # Able to select independent rows
                filterable=True,  # Able to filter columns by value
                sortable=True,  # Able to sort by column values
                editable=False,  # Not able to edit values
                resizable=True,  # Able to resize columns to fit data
                max_rows_in_viewport=5,  # Scroll if more than 5 rows
                selected_row_indices=selected_indices,  # inherit selected rows
                id='table-clowdrexp')  # Always use the same ID (for callbacks)


# Redraws the two-tab layout and table, preserving selected data. Tabs contain:
#  - Tab 1: Stats table
#  - Tab 2: Invocation table
def create_tabs_children(active_tab, selected_indices, rows):
    # If the table has reduced rows, we need to map the selected rows to tasks
    global_indices = get_global_index(rows, selected_indices)

    # For each tab, just recreate the table using the correct indices and data
    if active_tab == 'stats-tab':
        stat_child = [create_datatable(stat_dict, global_indices)]
        invo_child = []
    elif active_tab == 'invo-tab':
        invo_child = [create_datatable(invo_dict, global_indices)]
        stat_child = []

    # Put the table and empty table into the layout
    tabs = [# Stat table creation
            dcc.Tab(label="Statistics",
                    value='stats-tab',
                    children=stat_child),
            # Invocation table creation
            dcc.Tab(label="Invocations",
                    value='invo-tab',
                    children=invo_child)]
    return tabs


# Creates a figure template, used on launch, and can be edited during callbacks
main_colour = '#0074D9'
accent_colour = '#FF851B'
def create_figure():
    # Initialize plotting space
    fig = plotly.tools.make_subplots(rows=3, cols=1,
                                     subplot_titles=('Memory Usage',
                                                     'Task Launch Order',
                                                     'Memory Profile'),
                                     shared_xaxes=False)

    # Add every row of table to graph with the main colour
    id_list = []
    for i, exp in enumerate(experiment_dict):
        id_list += [exp['Task ID']]
        fig = append_trace(fig, exp, i)

    # Create a to-be-destroyed gantt chart just to steal the layout info
    tmpfig = ff.create_gantt([{'Task': exp['Task ID'],
                               'Start': exp['Time: Start'],
                               'Finish': exp['Time: End']}])
    gantt_lay = tmpfig['layout']
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }

    # Set layout info for RAM plot
    fig['layout']['yaxis1']['title'] = 'RAM (MB)'
    fig['layout']['yaxis1']['rangemode'] = 'tozero'
    fig['layout']['xaxis1']['title'] = 'Time (s)'

    # Set layout info for Gantt plot
    fig['layout']['yaxis2']['title'] = 'Task'
    fig['layout']['yaxis2']['autorange'] = 'reversed'
    fig['layout']['yaxis2']['zeroline'] = False
    fig['layout']['xaxis2']['title'] = 'Datetime'
    fig['layout']['xaxis2']['showgrid'] = False
    fig['layout']['xaxis2']['type'] = 'date'
    return fig


# Utility for adding a trace back to the graph
def append_trace(fig, data_row, idx, colour=main_colour):
    # RAM Time series plot
    fig.append_trace({
        'x': data_row['Time: Series (s)'],
        'y': data_row['RAM: Series (MB)'],
        'mode': 'lines+markers',
        'line': {'color': colour},
        'opacity': 0.6,
        'name': 'Task {}'.format(data_row['Task ID'])
    }, 1, 1)

    tmpfig = ff.create_gantt([{'Task': data_row['Task ID'],
                               'Start': data_row['Time: Start'],
                               'Finish': data_row['Time: End']}])
    gantt_dat = tmpfig['data'][0]
    gantt_dat['y'] = (idx, idx)
    gantt_dat['mode'] = 'lines'
    gantt_dat['line'] = {'color': colour, 'width': 5}
    gantt_dat['name'] = 'Task {}'.format(data_row['Task ID'])
    fig.append_trace(gantt_dat, 2, 1)

    # TODO: replace with table of std out/err info
    fig.append_trace({
        'x': data_row['Time: Series (s)'],
        'y': data_row['RAM: Series (MB)'],
        'mode': 'lines',
        'line': {'color': colour},
        'opacity': 0.8,
        'name': 'Task {}'.format(data_row['Task ID'])
    }, 3, 1)
    return fig


# Utility for getting the global index from index on subset
def get_global_index(rows, selected_indices):
    return [task_to_idx_lut[row['Task']]
            for idx, row in enumerate(rows)
            if idx in selected_indices]


# Utility to get all traces corresponding to a particular task ID
def get_trace_locations(traces, task_id):
    return [idx for idx, tr in enumerate(traces)
            if tr['name'] == 'Task {}'.format(task_id)]
# <------ /stop helper function creation


# Create page objects and their content
# ------> /start page creation
app.layout = html.Div([
    # Page title/header
    html.H4('Clowdr Experiment Explorer'),

    # Tabs & tables
    dcc.Tabs(id='tabs', value='stats-tab',
             children=create_tabs_children('stats-tab', [], stat_dict)),

    # Graph container (to be populated by callbacks)
    dcc.Graph(id='graph-clowdrexp',
              figure=create_figure())
    ], className="container")
# <------ /stop page creation


# Create callbacks for page elements
# ------> /start callback management
# Callback: update table based on changing selected tab & preserve selected rows
@app.callback(
    Output('tabs', 'children'),
    [Input('tabs', 'value')],
    [State('table-clowdrexp', 'selected_row_indices'),
     State('table-clowdrexp', 'rows')])
def redraw_table(tab, selected_indices, rows):
    return create_tabs_children(tab, selected_indices, rows)


# Callback: update selected rows based on available rows or click events
@app.callback(
    Output('table-clowdrexp', 'selected_row_indices'),
    [Input('graph-clowdrexp', 'clickData')],
    [State('table-clowdrexp', 'selected_row_indices')])
def update_selected_rows(clickData, selected_indices):
    if clickData:
        point = clickData['points'][0]
        curve = point['curveNumber'] // 3  # TODO: replace with # of graphs
        if curve in selected_indices:
            selected_indices.remove(curve)
        else:
            selected_indices.append(curve)
    return selected_indices

# Callback: update figure based on selected/present data
@app.callback(
    Output('graph-clowdrexp', 'figure'),
    [Input('table-clowdrexp', 'rows'),
     Input('table-clowdrexp', 'selected_row_indices')],
    [State('graph-clowdrexp', 'figure')])
def update_figure(rows, selected_indices, figure):
    # Grab traces from figure
    traces = figure['data']

    # Identify which traces to make invisible, get their locations, and do it
    row_ids = [r["Task"] for r in rows]
    hidden_ids = [exp['Task ID']
                  for exp in experiment_dict
                  if exp['Task ID'] not in row_ids]
    hidden_locs = [loc
                   for hid in hidden_ids
                   for loc in get_trace_locations(traces, hid)]

    # Identify which traces whould be which colour
    global_indices = get_global_index(rows, selected_indices)
    selected_ids = [experiment_dict[indx]['Task ID']
                    for indx in global_indices]
    selected_locs = [loc
                     for sid in selected_ids
                     for loc in get_trace_locations(traces, sid)]

    # Hide the hidden traces, and unhide the rest
    # Also, recolour based on if selected
    for loc in range(len(traces)):
        if loc in hidden_locs:
            traces[loc]['visible'] = False
        else:
            traces[loc]['visible'] = True

        if loc in selected_locs:
            traces[loc]['line']['color'] = accent_colour
        else:
            traces[loc]['line']['color'] = main_colour

    figure['data'] = traces
    return figure
# <------ /stop callback management


# Add CSS to page
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

# Set server launch when run as main app
if __name__ == '__main__':
    app.run_server(debug=True)

