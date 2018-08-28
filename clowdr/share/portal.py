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
        config = {'scrollZoom': True, # , 'toggleSpikeLines': True}
                  'modeBarButtonsToRemove': ['sendDataToCloud'],
                  'SpikeLines': True}
        self.app.layout = html.Div([
            # Page title/header
            html.H4('Clowdr Experiment Explorer'),

            # Tabs & tables
            dcc.Tabs(id='tabs', value='stats-tab',
                     children=self.create_tabs_children('stats-tab',
                                                        [],
                                                        self.stat_dict)),

            # Graph container (to be populated by callbacks)
            dcc.Graph(id='graph-clowdrexp',
                      figure=self.create_figure(),
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
            [State('table-clowdrexp', 'selected_row_indices')])
        def update_selected_rows(clickData, selected_indices):
            if clickData:
                point = clickData['points'][0]
                curve = point['curveNumber'] // 3  # TODO: replace # of graphs
                if curve in selected_indices:
                    selected_indices.remove(curve)
                else:
                    selected_indices.append(curve)
            return selected_indices

        # Callback: update figure based on selected/present data
        @self.app.callback(
            Output('graph-clowdrexp', 'figure'),
            [Input('table-clowdrexp', 'rows'),
             Input('table-clowdrexp', 'selected_row_indices')],
            [State('graph-clowdrexp', 'figure')])
        def update_figure(rows, selected_indices, figure):
            # Grab traces from figure
            traces = figure['data']

            # Identify which traces to make invisible, and do it
            row_ids = [r["Task"] for r in rows]
            hidden_ids = [exp['Task ID']
                          for exp in self.experiment_dict
                          if exp['Task ID'] not in row_ids]
            hidden_locs = [loc
                           for hid in hidden_ids
                           for loc in self.get_trace_locations(traces, hid)]

            # Identify which traces whould be which colour
            global_indices = self.get_global_index(rows, selected_indices)
            selected_ids = [self.experiment_dict[indx]['Task ID']
                            for indx in global_indices]
            selected_locs = [loc
                             for sid in selected_ids
                             for loc in self.get_trace_locations(traces, sid)]

            # Hide the hidden traces, and unhide the rest
            # Also, recolour based on if selected
            for loc in range(len(traces)):
                if loc in hidden_locs:
                    traces[loc]['visible'] = False
                else:
                    traces[loc]['visible'] = True

                if loc in selected_locs:
                    traces[loc]['line']['color'] = self.accent_colour
                else:
                    traces[loc]['line']['color'] = self.main_colour

            figure['data'] = traces
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

    def create_figure(self):
        # Initialize plotting space
        fig = plotly.tools.make_subplots(rows=3, cols=1,
                                         subplot_titles=('Memory Usage',
                                                         'Task Gantt',
                                                         'CPU Usage'),
                                         shared_xaxes=False)

        # Add every row of table to graph with the main colour
        id_list = []
        for i, exp in enumerate(self.experiment_dict):
            id_list += [exp['Task ID']]
            fig = self.append_trace(fig, exp, i, colour=self.main_colour)

        # Create a to-be-destroyed gantt chart just to steal the layout info
        tmpfig = ff.create_gantt([{'Task': exp['Task ID'],
                                   'Start': exp['Time: Start'],
                                   'Finish': exp['Time: End']}])
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
        fig['layout']['yaxis1']['spikedash'] = 'dash'
        fig['layout']['yaxis1']['spikesnap'] = 'data'
        fig['layout']['yaxis1']['spikemode'] = 'toaxis'
        fig['layout']['xaxis1']['title'] = 'Time (s)'
        fig['layout']['xaxis1']['spikemode'] = 'toaxis'

        # Set layout info for Gantt plot
        # RAM y-axis
        fig['layout']['yaxis3']['title'] = 'Max RAM (MB)'
        fig['layout']['yaxis3']['zeroline'] = True
        # Task y-axis
        # fig['layout']['yaxis2']['title'] = 'Task'
        # fig['layout']['yaxis2']['autorange'] = 'reversed'
        # fig['layout']['yaxis2']['zeroline'] = False
        fig['layout']['xaxis3']['title'] = 'Datetime'
        fig['layout']['xaxis3']['showgrid'] = False
        fig['layout']['xaxis3']['type'] = 'date'

        return fig

    # Utility for adding a trace back to the graph
    def append_trace(self, fig, data_row, idx, colour):
        # RAM Time series plot
        fig.append_trace({
            'x': data_row['Time: Series (s)'],
            'y': data_row['RAM: Series (MB)'],
            'mode': 'lines+markers',
            'line': {'color': colour},
            'opacity': 0.6,
            'xaxis': 'x1',
            'yaxis': 'y1',
            'name': 'Task {}'.format(data_row['Task ID'])
        }, 1, 1)

        fig.append_trace({
            'x': data_row['Time: Series (s)'],
            'y': data_row['CPU: Series (%)'],
            'mode': 'lines',
            'line': {'color': colour},
            'opacity': 0.8,
            'xaxis': 'x1',
            'yaxis': 'y2',
            'name': 'Task {}'.format(data_row['Task ID'])
        }, 2, 1)

        tmpfig = ff.create_gantt([{'Task': data_row['Task ID'],
                                   'Start': data_row['Time: Start'],
                                   'Finish': data_row['Time: End']}])
        gantt_dat = tmpfig['data'][0]
        # RAM y-axis
        gantt_dat['y'] = tuple([data_row['RAM: Max (MB)']] * 2)
        # Task y-axis
        # gantt_dat['y'] = (idx, idx)
        gantt_dat['mode'] = 'lines'
        gantt_dat['line'] = {'color': colour, 'width': 5}
        gantt_dat['name'] = 'Task {}'.format(data_row['Task ID'])
        fig.append_trace(gantt_dat, 3, 1)
        return fig

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
