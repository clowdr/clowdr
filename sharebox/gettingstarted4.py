import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import plotly.graph_objs as go


app = dash.Dash()


def generate_table(dff, title="",max_rows=20):
    if 'task' in dff.columns:
        dff = dff[dff['task'] == -1 ][['Invocation Param', 'Invocation Value']]
    return html.Div(
        [html.H5("Task1"),
        html.Table(
            # Header
            [html.Tr([html.Th(col) for col in dff.columns])] +

            # Body
            [html.Tr([
                html.Td(dff.iloc[i][col]) for col in dff.columns
            ]) for i in range(min(len(dff), max_rows))]
        )]
    )


df = pd.read_csv('sharebox/clowdr-summary.csv')

available_params = df['Invocation Param'].notnull().unique()
tasks = df['task'].unique()

times = [float(t) for t in df['time'].unique() if t > -1]
times = [int(t) for t in np.histogram(times, bins=12)[1]]

app.layout = html.Div([
    html.Div([
        html.Div([
            html.H5('Task(s)'),
            dcc.Dropdown(
                id='crossfilter-task',
                options=[{'label': i, 'value': i} for i in tasks],
                value=[i for i in tasks],
                multi=True,
            )
        ],
        style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            html.Div([
                html.H6('Y-Axis', style={'display':'inline-block'}),
                dcc.Dropdown(
                    id='crossfilter-yaxis-column',
                    options=[{'label': i.title(), 'value': i} for i in df.columns],
                    value='ram'),
                    dcc.RadioItems(
                        id='crossfilter-yaxis-type',
                        options=[{'label': i.title(), 'value': i}
                                 for i in ['linear', 'log', 'histogram']],
                        value='linear',
                        labelStyle={'display': 'inline-block'})
                    ], style={'width': '48%', 'float': 'left', 'display': 'inline-block'}),
            html.Div([
                html.H6('X-Axis', style={'display':'inline-block'}),
                dcc.Dropdown(
                    id='crossfilter-xaxis-column',
                    options=[{'label': i.title(), 'value': i} for i in df.columns],
                    value='time'),
                    dcc.RadioItems(
                        id='crossfilter-xaxis-type',
                        options=[{'label': i.title(), 'value': i}
                                 for i in ['linear', 'log']],
                        value='linear',
                        labelStyle={'display': 'inline-block'})
                    ], style={'width': '48%', 'float': 'right',
                       'display': 'inline-block'})
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block',
                  'clear': 'both'})
    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '10px 5px',
    }),
    html.Div([
        dcc.Graph(
            id="crossfilter-indicator-scatter",
            hoverData={'points': [{'customdata': 0}]}
        )
    ], style={'width': '48%', 'display': 'inline-block', 'padding': '10 20', 'float': 'left'}),
    html.Div([
        html.Div([
            generate_table(df[df['task'] == 0][['Invocation Param',
                                                 'Invocation Value']])],
            id="crossfilter-invotable",
            style={'padding':'20'}),
        dcc.Graph(id='x-time-series'),
    ], style={'display': 'inline-block', 'width': '48%', 'float': 'right'})
])

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})


# @app.callback(
#     dash.dependencies.Output('crossfilter-xaxis-type', 'option'),
#     [dash.dependencies.Input('crossfilter-xaxis-type', 'value')])
# def disable_xaxis_button(yaxis_type):
#     print('disable')
#     return yaxis_type == 'histogram'
# 
# @app.callback(
#     dash.dependencies.Output('crossfilter-xaxis-column', 'disabled'),
#     [dash.dependencies.Input('crossfilter-xaxis-type', 'value')])
# def disable_xaxis_column(yaxis_type):
#     print('disable')
#     return yaxis_type == 'histogram'


@app.callback(
    dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
    [dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-xaxis-type', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-type', 'value'),
     dash.dependencies.Input('crossfilter-task', 'value')])
def update_graph(xaxis_column_name, yaxis_column_name,
                 xaxis_type, yaxis_type, task_value):
    dff = df[df['task'].isin(task_value)]
    import colorlover as cl
    colours = cl.scales['11']['qual']['Paired']
    print(xaxis_column_name)

    dseries = []
    for task in task_value:
        dseries += [go.Scatter(
                        x=dff[df['task'] == task][xaxis_column_name],
                        y=dff[df['task'] == task][yaxis_column_name],
                        name='Task {}'.format(task),
                        customdata=[task] * len(dff[df['task']==task]),
                        mode='markers+lines',
                        marker={
                            'size': 6,
                            'opacity': 0.5,
                            'color': colours[task % 11]
                        })]

    return {'data': dseries,
            'layout': go.Layout(
                xaxis={
                       'title': xaxis_column_name,
                       'type': xaxis_type
                },
                yaxis={
                       'title': yaxis_column_name,
                       'type': yaxis_type
                },
                margin={'l': 40, 'b': 30, 't': 10, 'r': 0},
                height=450,
                hovermode='closest',
                showlegend=False,
            )}


def create_time_series(dff, title):
    return {
        'data': [go.Scatter(
            x=dff['Invocation Param'],
            y=dff['Invocation Value'],
            mode='lines+markers'
        )],
        'layout': {
            'height': 225,
            'margin': {'l': 20, 'b': 30, 'r': 10, 't': 10},
            'annotations': [{
                'x': 0, 'y': 0.85, 'xanchor': 'left', 'yanchor': 'bottom',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                'align': 'left', 'bgcolor': 'rgba(255, 255, 255, 0.5)',
                'text': title
            }],
            'yaxis': {'type': 'linear'},
            'xaxis': {'showgrid': False}
        }
    }


@app.callback(
    dash.dependencies.Output('crossfilter-invotable', 'children'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData')])
def update_invo_table(hoverData):
    task = hoverData['points'][0]['customdata']
    dff = df[df['task'] == task]
    dff = dff[dff['Invocation Param'].notnull()]
    dff = dff[['Invocation Param', 'Invocation Value']]
    print(dff)
    return generate_table(dff)


# @app.callback(
#     dash.dependencies.Output('y-time-series', 'figure'),
#     [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
#      dash.dependencies.Input('crossfilter-yaxis-column', 'value')])
# def update_x_timeseries(hoverData, yaxis_column_name):
#     print(hoverData)
#     dff = df[df['task'] == hoverData['customdata']]
#     dff = dff[dff['Indicator Name'] == yaxis_column_name]
#     return create_time_series(dff, yaxis_column_name)


if __name__ == '__main__':
    app.run_server(debug=True)

