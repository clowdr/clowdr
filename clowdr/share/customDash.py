#/usr/bin/env python

import dash

class CustomDash(dash.Dash):
    def interpolate_index(self, **kwargs):
        # Inspect the arguments by printing them
        return '''
<!DOCTYPE html>
<html>
    <head>
    <title>Clowdr Explorer</title>
    <link rel="stylesheet" href="https://unpkg.com/react-select@1.0.0-rc.3/dist/react-select.min.css">
    <link rel="stylesheet" href="https://unpkg.com/react-virtualized@9.9.0/styles.css">
    <link rel="stylesheet" href="https://unpkg.com/react-virtualized-select@3.1.0/styles.css">
    <link rel="stylesheet" href="https://unpkg.com/rc-slider@6.1.2/assets/index.css">
    <link rel="stylesheet" href="https://unpkg.com/dash-core-components@0.27.1/dash_core_components/react-dates@12.3.0.css">
    <link rel="stylesheet" href="https://unpkg.com/dash-table-experiments@0.6.0/dash_table_experiments/dash_table_experiments.css">
    <link rel="stylesheet" href="https://codepen.io/chriddyp/pen/bWLwgP.css">
    <link rel="stylesheet" href="/assets/banner.css?mod=1534778913.0">
    </head>
    <body>
        {app_entry}
        {config}
        {scripts}
    <footer>
    <script src="https://unpkg.com/react@15.4.2/dist/react.min.js"></script>
    <script src="https://unpkg.com/react-dom@15.4.2/dist/react-dom.min.js"></script>
    <script src="https://unpkg.com/dash-html-components@0.11.0/dash_html_components/bundle.js"></script>
    <script src="https://cdn.plot.ly/plotly-1.39.1.min.js"></script>
    <script src="https://unpkg.com/dash-core-components@0.27.1/dash_core_components/bundle.js"></script>
    <script src="https://unpkg.com/dash-table-experiments@0.6.0/dash_table_experiments/bundle.js"></script>
    <script src="https://unpkg.com/dash-renderer@0.13.0/dash_renderer/bundle.js"></script>
    </footer>
    </body>
</html>
        '''.format(
            app_entry=kwargs['app_entry'],
            config=kwargs['config'],
            scripts=kwargs['scripts'])

