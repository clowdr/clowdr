#!/usr/bin/env python

from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import dash
import flask
import urllib.parse
import io

import plotly.figure_factory as ff
import plotly.graph_objs as go
import plotly

from datetime import datetime, timedelta
from copy import deepcopy
import pandas as pd
import numpy as np
import json

from clowdr.share import customDash as cd


class CreatePortal():
    def __init__(self, experiment_dict, N=100):
        # Initialize Dash app with custom wrapper that lets us set page title
        # self.app = cd.CustomDash()
        # external CSS stylesheets
        external_stylesheets = [
            'https://codepen.io/chriddyp/pen/bWLwgP.css',
        ]
        self.app = dash.Dash(__name__,
                             external_stylesheets=external_stylesheets)
        self.app.title = "Clowdr Share Portal"
        self.app.config['supress_callback_exceptions'] = False
        # Load and groom dataset
        # -----> /start data grooming

        self.main_colour = '#0074D9'
        self.accent_colour = '#FF851B'
        self.fade_colour = '#66B8FF'

        # Intitialize two new data tables for visualization, resulting in:
        #   - (Full) experiment table, to be used in visualizations
        #   - Stats table, to be used for filtering based on summary info
        #   - Invocation table, to be used for filtering based on invoc. info
        self.stat_dict = []
        self.invo_dict = []
        new_experiment_dict = []
        for _task_id in range(len(experiment_dict)):
            exp = [tex
                   for tex in experiment_dict
                   if int(tex["Task ID"]) == int(_task_id)][0]
            # Coerce task ID into integer value
            exp['Task ID'] = int(exp['Task ID'])
            if exp['Time: Start'] is not None:
                time_s = datetime.strptime(exp['Time: Start'],
                                           "%Y-%m-%d %H:%M:%S")
                time_e = time_s + timedelta(0, exp['Time: Total (s)'])
            else:
                time_e = None
            exp['Time: End'] = str(time_e)

            if exp['Exit Code'] != "Incomplete":
                tmpexp_ram = exp['RAM: Series (MB)']
                tmpexp_tim = exp['Time: Series (s)']
                tmpexp_cpu = exp['CPU: Series (%)']

                timlin = np.linspace(tmpexp_tim[0], tmpexp_tim[-1], N)
                tmpexp_tim_resamp = []
                tidx = []
                for tl in timlin:
                    tmpexp_tim_resamp += [min(tmpexp_tim,
                                              key=lambda x: abs(x-tl))]
                    tidx += [tmpexp_tim.index(tmpexp_tim_resamp[-1])]

                tmpexp_ram_resamp = [tmpexp_ram[ti] for ti in tidx]
                tmpexp_cpu_resamp = [tmpexp_cpu[ti] for ti in tidx]

                exp['RAM: Series (MB)'] = tmpexp_ram_resamp
                exp['Time: Series (s)'] = tmpexp_tim_resamp
                exp['CPU: Series (%)'] = tmpexp_cpu_resamp

            # Add relevant info to stats dict
            self.stat_dict += [{'Task': exp['Task ID'],
                                'Tool': exp['Tool Name'],
                                'Duration (s)': exp['Time: Total (s)'],
                                'Max RAM (MB)': exp['RAM: Max (MB)'],
                                'Max CPU (%)': exp['CPU: Max (%)'],
                                'Exit Code': exp['Exit Code']}]

            # Add relevant info to invocation dict
            tmp_dict = {'Task': exp['Task ID']}
            tmp_dict.update({key.replace('Param: ', ''): exp[key]
                             for key in exp.keys()
                             if key.startswith('Param: ')})
            tmp_dict = {k: str(tmp_dict[k])
                        if type(tmp_dict[k]) == bool
                        else tmp_dict[k]
                        for k in tmp_dict.keys()}
            self.invo_dict += [tmp_dict]

            # Save the minor updates back to the dictionary
            new_experiment_dict += [exp]

        # Get rid of un-updated version of the dictionary
        self.experiment_dict = new_experiment_dict

        # Create item download dictionary
        self.downdict = [{'label': k, 'value': k}
                         for k in self.experiment_dict[0].keys()
                         if not k.startswith(("Param:",
                                              "CPU: Max",
                                              "RAM: Max"))]
        self.downdict = [{'label': k, 'value': k}
                         for k in ['Task ID', 'Exit Status', 'Output Logs',
                                   'Error Logs', 'Tool Name', 'RAM Usage',
                                   'CPU Usage', 'Timing', 'Parameters']]
        self.downloaders = [v['value'] for v in self.downdict]

        # Create look-up-table for mapping tasks to rows in the tables
        self.idx_to_task_lut = {exp['Task ID']: idx
                                for idx, exp in enumerate(self.experiment_dict)}
        self.task_to_idx_lut = {idx: exp['Task ID']
                                for idx, exp in enumerate(self.experiment_dict)}

        # <------ /stop data grooming

    def launch(self):
        self.create_dashboard()
        self.create_callbacks()
        return self.app

    def create_dashboard(self):
        # Create page objects and their content
        # ------> /start page creation
        config = {'modeBarButtonsToRemove': ['sendDataToCloud']}
        self.app.layout = html.Div([
            # Page title/header
            html.H4('Clowdr Experiment Explorer'),

            html.Div([
                html.Div([
                    # Tabs & tables
                    dcc.Tabs(id='tabs', value='stats-tab',
                             children=self.create_tabs_children('stats-tab',
                                                                [0],
                                                                self.stat_dict))
                ], className="nine columns"),

                html.Div([
                    html.Div([
                        # Utility bar (logs, download selected)
                        html.Div(id="DownloadText",
                                 children="Selected Task Data:"),
                        dcc.Dropdown(id='download-list',
                                     options=self.downdict,
                                     multi=True,
                                     value=self.downloaders)
                    ]),
                    html.A(
                        html.Button('Download',
                                    id="download-button",
                                    disabled=False,
                                    className="button",
                                    style={"margin-top": "10px",
                                           "color": "#ccc",
                                           "width": "100%"}),
                        id='download-button-link',
                        download="clowdr-experiment.json",
                        href="",
                        target="_blank"
                    )
                ], style={"position": "absolute", "bottom": "4px", "right": 0},
                  className="three columns")
            ], style={"position": "relative"}, className="row"),

            html.Div([
                html.Div([
                    # Gantt container (to be updated by callbacks)
                    dcc.Graph(id='gantt-clowdrexp',
                              figure=self.create_gantt(),
                              config=config),
                ], className="five columns"),

                html.Div([
                    # Graph container (to be populated by callbacks)
                    dcc.Graph(id='graph-clowdrexp',
                              figure=self.create_figure(self.stat_dict, [0]),
                              config=config)
                ], className="seven columns")
            ], className="row")
        ], className="container", style={"max-width": "1080px"})

        # <------ /stop page creation

    def create_callbacks(self):
        # Create callbacks for page elements
        # ------> /start callback management

        # Callback: update table based on tab & preserve selected rows
        @self.app.callback(
            Output('tabs', 'children'),
            [Input('tabs', 'value')],
            [State('table-clowdrexp', 'selected_row_indices'),
             State('table-clowdrexp', 'rows')])
        def redraw_table(tab, selected_indices, rows):
            return self.create_tabs_children(tab, selected_indices, rows)

        # Callback: update selected rows based on available rows or click event
        @self.app.callback(
            Output('table-clowdrexp', 'selected_row_indices'),
            [Input('gantt-clowdrexp', 'clickData')],
            [State('table-clowdrexp', 'selected_row_indices'),
             State('table-clowdrexp', 'rows'),
             State('table-clowdrexp', 'sortColumn'),
             State('table-clowdrexp', 'sortDirection')])
        def update_selected_rows(clickData, selected_indices, rows,
                                 sortcol, sortdir):
            row_ids = [r['Task'] for r in rows]

            if clickData:
                task = clickData['points'][0]['customdata']
                taskloc = row_ids.index(task)
                if task in row_ids:
                    if taskloc in selected_indices:
                        selected_indices.remove(taskloc)
                    else:
                        selected_indices.append(taskloc)
            return selected_indices

        # Callback: update figure based on selected/present data
        @self.app.callback(
            Output('graph-clowdrexp', 'figure'),
            [Input('table-clowdrexp', 'selected_row_indices')],
            [State('table-clowdrexp', 'rows')])
        def update_figure(selected_indices, rows):
            # Create new figure based on selected rows
            figure = self.create_figure(rows, selected_indices)
            return figure

        # Callback: update download button based on selected/preesent data
        @self.app.callback(
            Output('download-button', 'disabled'),
            [Input('table-clowdrexp', 'selected_row_indices'),
             Input('download-list', 'value')])
        def toggle_download(selected_indices, download_list_fields):
            self.downloaders = download_list_fields
            if len(selected_indices) > 0 and len(download_list_fields) > 0:
                return False
            else:
                return True

        # Callback: update download data based on selections
        @self.app.callback(
            Output('download-button-link', 'href'),
            [Input('table-clowdrexp', 'selected_row_indices'),
             Input('download-list', 'value')],
            [State('table-clowdrexp', 'rows')])
        def update_download_data(selected_indices, download_list_fields, rows):
            # Start by filtering down to tasks of interest
            row_ids = [r['Task'] for r in rows]
            selected_ids = [row_ids[sids] for sids in selected_indices]
            task_data = [exp
                         for ids in selected_ids
                         for exp in self.experiment_dict
                         if exp['Task ID'] == ids]

            # Now, filter out unwanted columns
            # TODO: refactor, please
            dlf = download_list_fields
            send_data = []
            for task_datum in task_data:
                tdat = deepcopy(task_datum)
                if 'Task ID' not in dlf:
                    del tdat['Task ID']
                if 'Exit Status' not in dlf:
                    del tdat['Exit Code']
                if 'Output Logs' not in dlf:
                    del tdat['Log: Output']
                else:
                    if tdat['Log: Output']:
                        tdat['Log: Output'] = tdat['Log: Output'].split('\n')
                if 'Error Logs' not in dlf:
                    del tdat['Log: Error']
                else:
                    if tdat['Log: Error']:
                        tdat['Log: Error'] = tdat['Log: Error'].split('\n')
                if 'Tool Name' not in dlf:
                    del tdat['Tool Name']
                if 'RAM Usage' not in dlf:
                    for tkey in [tk for tk in tdat.keys()
                                 if tk.startswith("RAM")]:
                        del tdat[tkey]
                if 'CPU Usage' not in dlf:
                    for tkey in [tk for tk in tdat.keys()
                                 if tk.startswith("CPU")]:
                        del tdat[tkey]
                if 'Timing' not in dlf:
                    for tkey in [tk for tk in tdat.keys()
                                 if tk.startswith("Time")]:
                        del tdat[tkey]
                if 'Parameters' not in dlf:
                    for tkey in [tk for tk in tdat.keys()
                                 if tk.startswith("Param")]:
                        del tdat[tkey]
                send_data += [tdat]

            tabstr = json.dumps(send_data)
            return "/dash/downloadExperiment?value={}".format(tabstr)

        # Callback: update button styling based on disabled/enabled setting
        @self.app.callback(
            Output('download-button', 'style'),
            [Input('download-button', 'disabled')],
            [State('download-button', 'style')])
        def change_button_appearance(disabled, style):
            if disabled:
                style['color'] = '#ccc'
            else:
                style['color'] = '#555'
            return style

        # Callback: update gantt based on selected/present data
        @self.app.callback(
            Output('gantt-clowdrexp', 'figure'),
            [Input('table-clowdrexp', 'selected_row_indices'),
             Input('table-clowdrexp', 'rows')],
            [State('gantt-clowdrexp', 'figure')])
        def update_gantt(selected_indices, rows, figure):
            figure = self.recolour_gantt(rows, selected_indices, figure)
            return figure
        # <------ /stop callback management

        # ------> /start custom app routes
        @self.app.server.route('/dash/downloadExperiment')
        def download_data():
            value = flask.request.args.get('value')
            bytIO = io.BytesIO()
            bytIO.write(json.dumps(json.loads(value),
                                   indent=4,
                                   sort_keys=True).encode('utf-8'))
            bytIO.seek(0)
            return flask.send_file(bytIO,
                                   mimetype='text/json',
                                   attachment_filename='clowdr-experiment.json',
                                   as_attachment=True)
    # <------ /stop custom app routes

    # Create helper functions to be used by callbacks & page creation
    # ------> /start helper function creation
    # Creates the data table given the dictionary and selected rows
    def create_datatable(self, data_dict, selected_indices):

        return dt.DataTable(
                    rows=data_dict,  # Table data
                    columns=list(data_dict[0].keys()),  # Column names
                    row_selectable=True,  # Able to select independent rows
                    filterable=True,  # Able to filter columns by value
                    # debounced=True,  # Delay after filtering before callback
                    sortable=True,  # Able to sort by column values
                    editable=False,  # Not able to edit values
                    resizable=True,  # Able to resize columns to fit data
                    max_rows_in_viewport=5,  # Scroll if more than 5 rows
                    selected_row_indices=selected_indices,  # selected rows
                    sortColumn='Task',  # set sorting by task
                    sortDirection='ASC',  # set increasing order for sort
                    id='table-clowdrexp')  # Use the same ID for callbacks

    # Redraws the two-tab layout and table, preserving selected data.
    #  - Tab 1: Stats table
    #  - Tab 2: Invocation table
    def create_tabs_children(self, active_tab, selected_indices, rows):
        # If the table has reduced rows, we need to map the rows to tasks
        global_indices = self.get_global_index(rows, selected_indices)

        # For each tab, recreate the table using the correct indices and data
        if active_tab == 'stats-tab':
            stat_child = [self.create_datatable(self.stat_dict, global_indices)]
            invo_child = []
        elif active_tab == 'invo-tab':
            invo_child = [self.create_datatable(self.invo_dict, global_indices)]
            stat_child = []

        # Put the table and empty table into the layout
        tabs = [
                # Stat table creation
                dcc.Tab(label="Statistics",
                        value='stats-tab',
                        children=stat_child),
                # Invocation table creation
                dcc.Tab(label="Invocations",
                        value='invo-tab',
                        children=invo_child)]
        return tabs

    # Creates gantt plot of tasks
    def create_gantt(self):
        # Create the generic gantt chart
        data = []
        for i, exp in enumerate(self.experiment_dict):
            if exp['Exit Code'] == 'Incomplete':
                continue
            tmpfig = ff.create_gantt([{'Task': exp['Task ID'],
                                       'Start': exp['Time: Start'],
                                       'Finish': exp['Time: End']}])
            t3 = tmpfig['data'][0]
            t3['y'] = (exp['Task ID'], exp['Task ID'])
            t3['customdata'] = [exp['Task ID']] * 2
            t3['hoverinfo'] = 'name'
            t3['mode'] = 'lines'
            t3['opacity'] = 0.7
            t3['line'] = {'color': self.main_colour, 'width': 5}
            t3['name'] = 'Task {}'.format(exp['Task ID'])
            data += [t3]

        layout = {
            'showlegend': False,
            'height': 300,
            'title': {
                'text': 'Experiment Timeline',
                'y': 0.9,
                'x': 0.5
            },
            'margin': {
                't': 60,
                'b': 60,
                'r': 50,
                'l': 50
            },
            'xaxis': {
                'anchor': 'y',
                'domain': [0, 1]
            },
            'yaxis': {
                'title': 'Task',
                'autorange': 'reversed',
                'zeroline': False,
                'domain': [0, 1],
            },
            'hovermode': 'closest'
        }
        fig = go.Figure(data, layout)
        return fig

    # Utility creating the main figure
    def create_figure(self, rows, selected_indices):
        row_ids = [r['Task'] for r in rows]
        selected_ids = [row_ids[sids] for sids in selected_indices]
        plotting_data = [exp
                         for ids in selected_ids
                         for exp in self.experiment_dict
                         if exp['Task ID'] == ids]

        # Initialize plotting space
        fig = plotly.tools.make_subplots(rows=2, cols=1,
                                         subplot_titles=('RAM Usage',
                                                         'CPU Usage'))
        data = []
        id_list = []
        opacity = max(0.02, 2/(len(plotting_data)+1))
        for i, exp in enumerate(plotting_data):
            if exp['Exit Code'] == 'Incomplete':
                continue
            id_list += [exp['Task ID']]
            data += self.append_trace(exp, i, opacity)

        if data == []:
            data = [{"yaxis": "y", "xaxis": "x",
                     'y': [100, 100], 'x': [0, 100], "opacity": 0,
                     "marker": {"opacity": 0}},
                    {"yaxis": "y2", "xaxis": "x",
                     'y': [100, 100], 'x': [0, 100], "opacity": 0,
                     "marker": {"opacity": 0}}]

        layout = {
            'showlegend': False,
            'height': 300,
            'title': {
                'text': 'Usage Stats',
                'y': 0.9,
                'x': 0.5
            },
            'margin': {
                't': 60,
                'b': 60,
                'r': 50,
                'l': 50
            },
            'xaxis': {
                'title': 'Time (s)',
                'domain': [0, 1]
            },
            'yaxis': {
                'title': 'RAM (MB)',
                'rangemode': 'tozero',
                'domain': [0.5, 1],
            },
            'yaxis2': {
                'title': 'CPU (%)',
                'autorange': 'reversed',
                'rangemode': 'tozero',
                'side': 'left',
                'domain': [0.0, 0.5],
            },
            'hovermode': 'closest'
        }

        fig = go.Figure(data=data, layout=layout)
        return fig

    # Utility for adding a trace back to the graph
    def append_trace(self, data_row, idx, opacity=0.6):
        # RAM Time series plot
        t1 = go.Scattergl(
            x=data_row['Time: Series (s)'],
            y=data_row['RAM: Series (MB)'],
            mode='lines+markers',
            line={'color': self.main_colour},
            opacity=opacity,
            marker={"opacity": opacity},
            fill='tozeroy',
            xaxis='x',
            yaxis='y',
            customdata=['ram'] * len(data_row['Time: Series (s)']),
            name='Task {}'.format(data_row['Task ID'])
        )

        # CPU Time series plot
        t2 = go.Scattergl(
            x=data_row['Time: Series (s)'],
            y=data_row['CPU: Series (%)'],
            mode='lines+markers',
            line={'color': self.accent_colour},
            opacity=opacity,
            marker={"opacity": opacity},
            fill='tozeroy',
            xaxis='x',
            yaxis='y2',
            customdata=['cpu'] * len(data_row['Time: Series (s)']),
            name='Task {}'.format(data_row['Task ID'])
        )

        data = [t1, t2]
        return data

    # Utility for recolouring Gantt traces
    def recolour_gantt(self, rows, selected_indices, figure):
        row_ids = [r['Task'] for r in rows]
        selected_ids = [row_ids[sids] for sids in selected_indices]
        traces = figure['data']
        newtraces = []
        for i, trace in enumerate(traces):
            if trace['customdata'][0] in selected_ids:
                tcolour = self.accent_colour
                opacity = 0.9
            elif trace['customdata'][0] not in row_ids:
                tcolour = self.fade_colour
                opacity = 0.25
            else:
                tcolour = self.main_colour
                opacity = 0.7
            trace['line'] = {'color': tcolour, 'width': 5}
            trace['opacity'] = opacity
            newtraces += [trace]

        figure['data'] = newtraces
        return figure

    # Utility for getting the global index from index on subset
    def get_global_index(self, rows, selected_indices):

        return [self.task_to_idx_lut[row['Task']]
                for idx, row in enumerate(rows)
                if idx in selected_indices]

    # Utility to get all traces corresponding to a particular task ID
    def get_trace_locations(self, traces, task_id):

        return [idx for idx, tr in enumerate(traces)
                if tr['name'] == 'Task {}'.format(task_id)]
    # <------ /stop helper function creation
