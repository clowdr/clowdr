import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import plotly.graph_objs as go

app = dash.Dash()

df = pd.read_csv('sharebox/clowdr-summary.csv')

print(df[:3])

available_params = df['Invocation Param'].unique()
print(available_params)

tasks = df['task'].unique()
print(tasks)

times = [float(t) for t in df['time'].unique() if t > -1]
times = [int(t) for t in np.histogram(times, bins=12)[1]]

app.layout = html.Div([
    html.Div([
        html.Div([
            html.H5('Task(s)'),
            dcc.Dropdown(
                id='crossfilter-task',
                options=[{'label': i, 'value': i} for i in tasks],
                value=[{'label': i, 'value': i} for i in tasks],
                multi=True,
            )
        ],
        style={'width': '49%', 'display': 'inline-block'}),
        html.Div([
            html.Div([
                html.H6('X-Axis:', style={'display':'inline'}),
                dcc.Dropdown(
                    id='crossfilter-xaxis-column',
                    options=[{'label': i.title(), 'value': i} for i in df.columns],
                    value='time'
            )], style={'width': '100%', 'float': 'right', 'display': 'inline-block'}),

            html.Div([
                html.H6('Y-Axis:', style={'display':'inline'}),
                dcc.Dropdown(
                    id='crossfilter-yaxis-column',
                    options=[{'label': i.title(), 'value': i} for i in df.columns],
                    value='ram'
            )], style={'width': '100%', 'float': 'right', 'display': 'inline-block'})
        ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'})
    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '10px 5px'
    }),

    html.Div([
        dcc.Graph(
            id='crossfilter-indicator-scatter',
            hoverData={'points': [{'task': '0'}]}
        )
    ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Graph(id='x-time-series'),
        dcc.Graph(id='y-time-series'),
    ], style={'display': 'inline-block', 'width': '49%'}),

    html.Div(dcc.Slider(
        id='crossfilter-year--slider',
        min=times[0],
        max=times[-1],
        value=times[-1],
        step=None,
        marks={str(time): str(time) for time in times}
    ), style={'width': '49%', 'padding': '0px 20px 20px 20px'})
])

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

@app.callback(
    dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
    [dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-xaxis-column', 'value')])
def update_graph(xaxis_column_name, yaxis_column_name, task_value):
    dff = df[df['task'] == task_value]
    print(dff)

    return {
        'data': [go.Scatter(
            x=dff[dff[''] == xaxis_column_name]['Value'],
            y=dff[dff['Indicator Name'] == yaxis_column_name]['Value'],
            text=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'],
            customdata=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            }
        )],
        'layout': go.Layout(
            xaxis={
                'title': xaxis_column_name,
                'type': 'linear'
            },
            yaxis={
                'title': yaxis_column_name,
                'type': 'linear'
            },
            margin={'l': 40, 'b': 30, 't': 10, 'r': 0},
            height=450,
            hovermode='closest'
        )
    }


def create_time_series(dff, title):
    return {
        'data': [go.Scatter(
            x=dff['Year'],
            y=dff['Value'],
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
    dash.dependencies.Output('x-time-series', 'figure'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
     dash.dependencies.Input('crossfilter-xaxis-column', 'value')])
def update_y_timeseries(hoverData, xaxis_column_name, axis_type):
    country_name = hoverData['points'][0]['customdata']
    dff = df[df['Country Name'] == country_name]
    dff = dff[dff['Indicator Name'] == xaxis_column_name]
    title = '<b>{}</b><br>{}'.format(country_name, xaxis_column_name)
    return create_time_series(dff, title)


@app.callback(
    dash.dependencies.Output('y-time-series', 'figure'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
     dash.dependencies.Input('crossfilter-yaxis-column', 'value')])
def update_x_timeseries(hoverData, yaxis_column_name):
    dff = df[df['Country Name'] == hoverData['points'][0]['customdata']]
    dff = dff[dff['Indicator Name'] == yaxis_column_name]
    return create_time_series(dff, yaxis_column_name)


if __name__ == '__main__':
    app.run_server(debug=True)


