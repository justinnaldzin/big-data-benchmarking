import os
import sys
import logging
import pandas
from bokeh.charts import Scatter, Bar, Line, Histogram
from bokeh.embed import file_html
from bokeh.resources import CDN
from bokeh.layouts import column, row


def results(csv_filepath):
    if not os.path.isfile(csv_filepath):
        logging.error("File: " + csv_filepath + " does not exist!")
        sys.exit(1)
    dataframe = pandas.read_csv(csv_filepath, low_memory=False)
    bootstrap_css, avg_query_time_html = avg_query_time(dataframe)



    for c in dataframe['concurrency_factor'].sort_values().unique():
        avg_query_time_dataframe = dataframe[dataframe['concurrency_factor'] == c].pivot_table(index=['query_id', 'name'], columns='database', values='time',
                                                         aggfunc='mean').fillna('')
        database_columns = avg_query_time_dataframe.columns
        avg_query_time_dataframe.reset_index(inplace=True)
        logging.info(avg_query_time_dataframe)
        query_line = Line(avg_query_time_dataframe, x='query_id', y=[col for col in database_columns],
                     xlabel='Query ID', ylabel='Time in seconds', color=[col for col in database_columns],
                     legend='top_right', legend_sort_field='color', title="Avg execution time of query by database")


    # HTML
    bokeh_html = file_html(column(row(None)),
                     title='Big Data Benchmarking', resources=CDN)

    output_file = 'Big-Data-Benchmarking.html'
    with open(output_file, mode='w', encoding='utf-8') as f:
        f.write(bootstrap_css + avg_query_time_html + bokeh_html)
    logging.info("Plotting results written to HTML file:  " + output_file)


def avg_query_time(dataframe):
    # Generate a table showing the average query times per database
    avg_query_time_dataframe = dataframe.pivot_table(index=['query_id', 'name'], columns='database', values='time',
                                                     aggfunc='mean').fillna('')
    avg_query_time_html = avg_query_time_dataframe.style\
        .set_table_attributes('class="table table-hover table-striped"')\
        .set_table_styles([dict(selector="caption", props=[("caption-side", "top")])])\
        .set_caption("Database query execution times")\
        .highlight_min(axis=1, color='#dff0d8')\
        .highlight_max(axis=1, color='#f2dede')\
        .render()
    bootstrap_css = '''<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/css/bootstrap.min.css" integrity="sha384-rwoIResjU2yc3z8GV/NPeZWAv56rSmLldC3R/AZzGRnGxQQKnKkoFVhFQhNUwEyJ" crossorigin="anonymous">
    <script src="https://code.jquery.com/jquery-3.1.1.slim.min.js" integrity="sha384-A7FZj7v+d/sdmMqp/nOQwliLvUsJfDHW+k9Omg/a/EheAdgtzNs3hpfag6Ed950n" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/tether/1.4.0/js/tether.min.js" integrity="sha384-DztdAPBWPRXSA/3eYEEUWrWCy7G5KFbe8fFjk5JAIxUYHKkDx6Qin1DkWx51bBrb" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/js/bootstrap.min.js" integrity="sha384-vBWWzlZJ8ea9aCX4pEW3rVHjgjt7zpkNpZk+02D9phzyeVkE+jo0ieGizqPLForn" crossorigin="anonymous"></script>'''
    return (bootstrap_css, avg_query_time_html)


def test_html_layout_testing(dataframe):
    # testing with HTML layout
    import io
    from jinja2 import Template
    from bokeh.embed import file_html
    from bokeh.layouts import column
    from bokeh.resources import JSResources
    from bokeh.util.browser import view

    layout = column(plot)
    # Open our custom template
    with open('bokeh_layout/template.jinja', 'r') as f:
        template = Template(f.read())

    # Use inline resources, render the html and open
    js_resources = JSResources(mode='inline')
    title = "Bokeh - title Plot"
    html = file_html(layout, resources=(js_resources, None), title=title, template=template)

    output_file = 'bokeh_layout_test.html'
    with io.open(output_file, mode='w', encoding='utf-8') as f:
        f.write(html)
    view(output_file)

