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


    # LINE CHART
    avg_query_time_dataframe = dataframe.pivot_table(index=['concurrency_factor'], columns='database', values='time',
                                                     aggfunc='mean').fillna('')
    database_columns = avg_query_time_dataframe.columns
    avg_query_time_dataframe.reset_index(inplace=True)
    '''
    line2 = Line(avg_query_time_dataframe, x='concurrency_factor', y=[col for col in database_columns],
                 xlabel='Concurrency Factor', ylabel='Time in seconds', color=[col for col in database_columns],
                 legend='top_right',
                 width=1800, legend_sort_field='color', title="Avg execution time of query by concurrency factor")
    '''
    query_line_list = []
    query_bar_list = []
    concurrency_scatter_plot_list = []
    database_bar_list = []
    concurrency_bar_list = []
    for c in dataframe['concurrency_factor'].sort_values().unique():


        ############
        avg_query_time_dataframe = dataframe[dataframe['concurrency_factor'] == c].pivot_table(index=['query_id', 'name'], columns='database', values='time',
                                                         aggfunc='mean').fillna('')
        database_columns = avg_query_time_dataframe.columns
        avg_query_time_dataframe.reset_index(inplace=True)
        logging.info(avg_query_time_dataframe)
        query_line = Line(avg_query_time_dataframe, x='query_id', y=[col for col in database_columns],
                     xlabel='Query ID', ylabel='Time in seconds', color=[col for col in database_columns],
                     legend='top_right', legend_sort_field='color', title="Avg execution time of query by database")
        ###############


        query_bar = Bar(dataframe[dataframe['concurrency_factor'] == c], label='query_id', values='time', group='database', legend='top_right', xlabel='Query ID',
                   ylabel='Time in seconds', title="Avg execution time by Query ID", agg='mean')

        concurrency_scatter_plot = Scatter(dataframe[dataframe['concurrency_factor'] == c], x='rows', y='time',
                                            color='database',
                                           title="Concurrency " + str(c) + " | Individual execution time of query vs rows",
                                           legend='top_left', legend_sort_field='color',
                                           legend_sort_direction='ascending', xlabel='Number of rows',
                                           ylabel='Time in seconds')

        concurrency_bar = Bar(dataframe[dataframe['concurrency_factor'] == c], values='time', label='concurrency_factor',
                              group='database', legend='top_right', xlabel='Concurrency factor', ylabel='Time in seconds',
                              title="Concurrency " + str(c) + " | Avg execution time of query by concurrency factor", agg='mean')


        database_bar = Bar(dataframe[dataframe['concurrency_factor'] == c], values='time', label='database',
                           stack='category', legend='top_right', xlabel='Database', ylabel='Time in seconds',
                              title="Concurrency " + str(c) + " | Avg execution time of query by database", agg='mean')
        query_line_list.append(query_line)
        query_bar_list.append(query_bar)
        concurrency_scatter_plot_list.append(concurrency_scatter_plot)
        database_bar_list.append(database_bar)
        concurrency_bar_list.append(concurrency_bar)

    # HTML
    bokeh_html = file_html(column(row(query_line_list), row(query_bar_list), row(concurrency_scatter_plot_list), row(database_bar_list), row(concurrency_bar_list)),
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

