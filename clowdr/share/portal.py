#!/usr/bin/env python

from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import dash

import plotly.figure_factory as ff
import plotly.graph_objs as go
import plotly

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

from clowdr.share import customDash as cd


class CreatePortal():
    def __init__(self, experiment_dict):
        # Initialize Dash app with custom wrapper that lets us set page title
        self.app = cd.CustomDash()
        # Load and groom dataset
        # -----> /start data grooming

        self.main_colour = '#0074D9'
        self.accent_colour = '#FF851B'

        # Intitialize two new data tables for visualization, resulting in:
        #   - (Full) experiment table, to be used in visualizations
        #   - Stats table, to be used for filtering based on summary info
        #   - Invocation table, to be used for filtering based on invoc. info
        self.stat_dict = []
        self.invo_dict = []
        new_experiment_dict = []
        for exp in experiment_dict:
            # Coerce task ID into integer value
            exp['Task ID'] = int(exp['Task ID'])
            if exp['Time: Start'] is not None:
                time_s = datetime.strptime(exp['Time: Start'], "%Y-%m-%d %H:%M:%S")
                time_e = time_s + timedelta(0, exp['Time: Total (s)'])
            else:
                time_e = None
            exp['Time: End'] = str(time_e)

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

        # Create look-up-table for mapping tasks to rows in the tables
        self.idx_to_task_lut = {exp['Task ID']: idx
                                for idx, exp in enumerate(experiment_dict)}
        self.task_to_idx_lut = {idx: exp['Task ID']
                                for idx, exp in enumerate(experiment_dict)}
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

            # Tabs & tables
            dcc.Tabs(id='tabs', value='stats-tab',
                     children=self.create_tabs_children('stats-tab',
                                                        [0],
                                                        self.stat_dict)),

            # Gantt container (to be updated by callbacks)
            # dcc.Graph(id='gantt-clowdrexp',
            #           figure=self.create_gantt(self.stat_dict, [0]),
            #           config=config),
            # TODO create gantt function; new callback to update gantt area

            # Graph container (to be populated by callbacks)
            dcc.Graph(id='graph-clowdrexp',
                      figure=self.create_figure(self.stat_dict, [0]),
                      config=config)
            ], className="container")
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
            [Input('graph-clowdrexp', 'clickData')],
            [State('table-clowdrexp', 'selected_row_indices'),
             State('table-clowdrexp', 'rows')])
        def update_selected_rows(clickData, selected_indices, rows):
            # global_indices = self.get_global_index(rows, selected_indices)
            if clickData:
                point = clickData['points'][0]
                if point['customdata'] == 'task':
                    curve = point['curveNumber'] - (len(selected_indices) * 2)
                    if curve in selected_indices:
                        selected_indices.remove(curve)
                    else:
                        selected_indices.append(curve)
            return selected_indices

        # Callback: update figure based on selected/present data
        @self.app.callback(
            Output('graph-clowdrexp', 'figure'),
            [Input('table-clowdrexp', 'selected_row_indices')],
            [State('table-clowdrexp', 'rows')])
        def update_figure(selected_indices, rows):
            # Create new figure based on selected rows
            global_indices = self.get_global_index(rows, selected_indices)
            figure = self.create_figure(rows, global_indices)
            return figure
    # <------ /stop callback management

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

    def create_figure(self, rows, global_indices):
        plotting_data = [self.experiment_dict[idx] for idx in global_indices]
        row_ids = [r['Task'] for r in rows]

        # Initialize plotting space
        fig = plotly.tools.make_subplots(rows=3, cols=1,
                                         subplot_titles=('RAM Usage',
                                                         'CPU Usage',
                                                         'Tasks'))

        data = []
        id_list = []
        opacity = 2/(len(plotting_data)+1)
        for i, exp in enumerate(plotting_data):
            if exp['Exit Code'] == 'Incomplete':
                continue
            id_list += [exp['Task ID']]
            #1 fig = self.append_trace(fig, exp, i)
            data += self.append_trace(exp, i, opacity)

        # Create the generic gantt chart
        ganttdat = []
        for i, exp in enumerate(self.experiment_dict):
            if exp['Exit Code'] == 'Incomplete':
                continue
            tmpfig = ff.create_gantt([{'Task': exp['Task ID'],
                                       'Start': exp['Time: Start'],
                                       'Finish': exp['Time: End']}])
            t3 = tmpfig['data'][0]
            t3['y'] = (i, i)
            t3['yaxis'] = 'y3'
            t3['customdata'] = ['task'] * 2
            t3['hoverinfo'] = 'name'
            t3['xaxis'] = 'x2'
            t3['mode'] = 'lines'
            if i in global_indices:
                tcolour = self.accent_colour
            else:
                tcolour = self.main_colour

            if exp['Task ID'] in row_ids:
                twidth = 5
            else:
                twidth = 2
            t3['line'] = {'color': tcolour, 'width': twidth}
            t3['name'] = 'Task {}'.format(exp['Task ID'])
            ganttdat += [t3]

        data += ganttdat
        layout = {
            'showlegend': False,
            'height': 600,
            'xaxis': {
                'title': 'Time (s)',
                'domain': [0, 1]
            },
            'xaxis2': {
                'title': 'Datetime',
                'anchor': 'y3',
                'domain': [0, 1]
            },
            'yaxis': {
                'title': 'RAM (MB)',
                'rangemode': 'tozero',
                'domain': [0.35, 0.7]
            },
            'yaxis2': {
                'title': 'CPU (%)',
                'autorange': 'reversed',
                'rangemode': 'tozero',
                'side': 'left',
                'domain': [0.0, 0.35]
            },
            'yaxis3': {
                'title': 'Task',
                'autorange': 'reversed',
                'zeroline': False,
                'domain': [0.85, 1],
            },
            'hovermode': 'closest'
        }

        fig = go.Figure(data, layout)
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
            fill='tozeroy',
            xaxis='x',
            yaxis='y2',
            customdata=['cpu'] * len(data_row['Time: Series (s)']),
            name='Task {}'.format(data_row['Task ID'])
        )

        data = [t1, t2]
        return data

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
