import os
import sys
import random
import pandas
from collections import defaultdict
from bokeh.charts import Bar, Scatter, Line
from bokeh.layouts import widgetbox, row, column
import bokeh.palettes
from bokeh.models import Button, Select, RangeSlider, DataTable, ColumnDataSource, CustomJS, TableColumn, Div, CheckboxGroup, MultiSelect, HoverTool
from bokeh.io import curdoc


# Randomly choose a palette from a pre-defined list
palettes = bokeh.palettes.all_palettes
palettes_list = ['Category20', 'Accent', 'Paired', 'Pastel1', 'Spectral', 'Set3']
palette_name = random.choice(list(palettes_list))
print("palette_name:  " + str(palette_name))
palette = palettes[palette_name][max(palettes[palette_name])]

# CSS
bokeh_css = '''
<link href="http://cdn.pydata.org/bokeh/release/bokeh-0.12.4.min.css" rel="stylesheet" type="text/css">
<link href="http://cdn.pydata.org/bokeh/release/bokeh-widgets-0.12.4.min.css" rel="stylesheet" type="text/css">
<script src="http://cdn.pydata.org/bokeh/release/bokeh-0.12.4.min.js"></script>
<script src="http://cdn.pydata.org/bokeh/release/bokeh-widgets-0.12.4.min.js"></script>
'''
bootstrap_css = '''
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/css/bootstrap.min.css" integrity="sha384-rwoIResjU2yc3z8GV/NPeZWAv56rSmLldC3R/AZzGRnGxQQKnKkoFVhFQhNUwEyJ" crossorigin="anonymous">
<script src="https://code.jquery.com/jquery-3.1.1.slim.min.js" integrity="sha384-A7FZj7v+d/sdmMqp/nOQwliLvUsJfDHW+k9Omg/a/EheAdgtzNs3hpfag6Ed950n" crossorigin="anonymous"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/tether/1.4.0/js/tether.min.js" integrity="sha384-DztdAPBWPRXSA/3eYEEUWrWCy7G5KFbe8fFjk5JAIxUYHKkDx6Qin1DkWx51bBrb" crossorigin="anonymous"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/js/bootstrap.min.js" integrity="sha384-vBWWzlZJ8ea9aCX4pEW3rVHjgjt7zpkNpZk+02D9phzyeVkE+jo0ieGizqPLForn" crossorigin="anonymous"></script>
'''


def update(attrname, old, new):
    # print(str(attrname) + ':  ' + str(old) + ' -> ' + str(new))
    # print("concurrency_factor:  " + str(select_concurrency.value))
    # print("time:  " + str(slider_time.range[0]) + " - " + str(slider_time.range[1]))
    # print("database:  " + str([checkbox_database.labels[index] for index in checkbox_database.active]))
    # print("query_id:  " + str(multiselect_query_id.value))
    # print("rows:  " + str(slider_rows.range[0]) + " - " + str(slider_rows.range[1]))
    current = dataframe[(dataframe['concurrency_factor'].astype(str) == select_concurrency.value)
                        & (dataframe['time'] >= slider_time.range[0])
                        & (dataframe['time'] <= slider_time.range[1])
                        & (dataframe['database'].astype(str).isin([checkbox_database.labels[index] for index in checkbox_database.active]))
                        & (dataframe['query_id'].astype(str).isin(multiselect_query_id.value))
                        & (dataframe['rows'] >= slider_rows.range[0])
                        & (dataframe['rows'] <= slider_rows.range[1])]
    source.data = ColumnDataSource(data=current).data


# Load source data
csv_filepath = os.path.join(os.path.dirname(__file__), 'big_data_benchmarking_20170125.csv')
if not os.path.isfile(csv_filepath):
    print("ERROR - File: " + csv_filepath + " does not exist!")
    sys.exit(1)
print("Reading file: " + csv_filepath + "...")
dataframe = pandas.read_csv(csv_filepath, low_memory=False)
source = ColumnDataSource(data=dataframe)

# DataTable displays the raw output from the CSV file.
columns = [TableColumn(field=c, title=c) for c in dataframe.columns]
data_table = DataTable(source=source, columns=columns, editable=False, width=1400, height=500)

# BarChart displaying the average query execution time for each database based on the concurrency factor.
bar_concurrency_by_database = Bar(source.to_df(), values='time', label='concurrency_factor', group='database',
                                  legend='top_right', xlabel='Concurrency factor', ylabel='Time in seconds',
                                  title="Average query execution time by concurrency factor", agg='mean', width=1800,
                                  tooltips=[('Database', '@database'), ('Time', '$y{0.000}'),
                                            ('Concurrency Factor', '@concurrency_factor')], palette=palette)
bar_concurrency_by_database.title.text_font_size = '12pt'

plots = defaultdict(list)
for i in dataframe['concurrency_factor'].sort_values().unique():
    # ScatterChart displays a point for every query comparing the execution time vs the number of rows returned from the query.
    scatter_plot = Scatter(dataframe[dataframe['concurrency_factor'] == i], x='rows', y='time', color='database',
                           title="Concurrency " + str(i) + "  |  Individual query execution time", legend='top_left',
                           legend_sort_field='color', legend_sort_direction='ascending', xlabel='Number of rows',
                           ylabel='Time in seconds', tooltips=[('Database', '@database'), ('Rows', '@rows'),
                                                               ('Time', '@time'), ('Category', '@category'),
                                                               ('Concurrency Factor', '@concurrency_factor'),
                                                               ('Query ID', '@query_id'),
                                                               ('Table Size', '@table_size_category')], palette=palette)
    scatter_plot.title.text_font_size = '12pt'
    plots['scatter_plot'].append(scatter_plot)

    # BarChart displaying the average query execution time for each database based on the query category.
    bar_database_by_query_category = Bar(dataframe[dataframe['concurrency_factor'] == i], values='time',
                                         label='database', stack='category', legend='top_right', xlabel='Database',
                                         ylabel='Time in seconds',
                                         title="Concurrency " + str(i) + " | Average query execution time by database",
                                         agg='mean', tooltips=[('Database', '@database'), ('Time', '$y{0.000}'),
                                                               ('Category', '@category')],
                                         palette=palette)
    bar_database_by_query_category.title.text_font_size = '12pt'
    plots['bar_database_by_query_category'].append(bar_database_by_query_category)

    # BarChart displaying the average query execution time for each database based on the table size.
    bar_database_by_table_size = Bar(dataframe[dataframe['concurrency_factor'] == i], values='time', label='database',
                                     stack='table_size_category', legend='top_right', xlabel='Database',
                                     ylabel='Time in seconds',
                                     title="Concurrency " + str(i) + " | Average query execution time by table size",
                                     agg='mean', tooltips=[('Database', '@database'), ('Time', '$y{0.000}'),
                                                           ('Table Size', '@table_size_category')], palette=palette)
    bar_database_by_table_size.title.text_font_size = '12pt'
    plots['bar_database_by_table_size'].append(bar_database_by_table_size)

    # BarChart displaying the average query execution time for each query based on the database.
    bar_query_id_by_database = Bar(dataframe[dataframe['concurrency_factor'] == i], label='query_id', values='time',
                                   group='database', legend='top_right', xlabel='Query ID', ylabel='Time in seconds',
                                   title="Concurrency " + str(i) + " | Average execution time by query id", agg='mean',
                                   tooltips=[('Database', '@database'), ('Time', '$y{0.000}'),
                                             ('Query ID', '@query_id')], palette=palette)
    bar_query_id_by_database.title.text_font_size = '12pt'
    plots['bar_query_id_by_database'].append(bar_query_id_by_database)

    # LineChart displaying the average query execution time for each query based on the database.
    avg_query_time_dataframe = dataframe[dataframe['concurrency_factor'] == i].pivot_table(
        index=['query_id', 'name'], columns='database', values='time', aggfunc='mean').fillna('')
    database_columns = avg_query_time_dataframe.columns
    avg_query_time_dataframe.reset_index(inplace=True)
    line_query_id_by_database = Line(avg_query_time_dataframe, x='query_id', y=[col for col in database_columns],
                                     xlabel='Query ID', ylabel='Time in seconds',
                                     color=[col for col in database_columns], legend='top_right',
                                     legend_sort_field='color',
                                     title="Concurrency " + str(i) + " Average execution time by query id",
                                     tooltips=[('Query ID', '@query_id'), ('Time', '$y{0.000}')], palette=palette)
    line_query_id_by_database.title.text_font_size = '12pt'
    plots['line_query_id_by_database'].append(line_query_id_by_database)

    # DataTable summarizing the average query execution times per database.
    avg_query_time_dataframe = dataframe[dataframe['concurrency_factor'] == i].pivot_table(
        index=['query_id', 'name'], columns='database', values='time', aggfunc='mean').fillna('')
    avg_query_time_html = avg_query_time_dataframe.style \
        .set_table_attributes('class="table table-sm table-hover table-striped"') \
        .set_table_styles([dict(selector="caption", props=[("caption-side", "top")])]) \
        .set_caption("Concurrency " + str(i) + " | Average query execution time per database") \
        .highlight_min(axis=1, color='#dff0d8') \
        .highlight_max(axis=1, color='#f2dede') \
        .render()
    plots['html_avg_query_time'].append(Div(text=bootstrap_css + bokeh_css + avg_query_time_html, width=1800))

    # # Write individual HTML files
    # output_file = 'average_query_time_concurrency_' + str(i) + '.html'
    # with open(output_file, mode='w', encoding='utf-8') as f:
    #     f.write(bootstrap_css + bokeh_css + avg_query_time_html)
    #
    # # DataTable summarizing the average query execution times per database.
    # div_avg_query_time = Div(text="<h6><b>Concurrency " + str(i) + " | Average query execution time per database</b></h6>", width=900)
    # plots['div_avg_query_time'].append(widgetbox(div_avg_query_time))
    # avg_query_time_dataframe = dataframe[dataframe['concurrency_factor'] == i].pivot_table(index=['query_id', 'name'], columns='database', values='time',
    #                                                  aggfunc='mean').fillna('').reset_index()
    # columns = [TableColumn(field=c, title=c) for c in avg_query_time_dataframe.columns]
    # datatable_avg_query_time = DataTable(source=ColumnDataSource(data=avg_query_time_dataframe), columns=columns,
    #                           editable=False, width=1800)
    # plots['datatable_avg_query_time'].append(widgetbox(datatable_avg_query_time))

# Widgets
options = list(dataframe['concurrency_factor'].sort_values().apply(str).unique())
select_concurrency = Select(title="Concurrency Factor:", value=options[0], options=options)
select_concurrency.on_change('value', update)

labels = list(dataframe['database'].sort_values().apply(str).unique())
checkbox_database = CheckboxGroup(labels=labels, active=[index for index, item in enumerate(labels)])
checkbox_database.on_change('active', update)

options = list(dataframe['query_id'].sort_values().apply(str).unique())
multiselect_query_id = MultiSelect(title="Query ID:", value=options, options=list(zip(options, options)), size=5)
multiselect_query_id.on_change('value', update)

end = dataframe['rows'].max()
slider_rows = RangeSlider(start=0, end=end, range=(0, end), step=end//100, title="Rows")
slider_rows.on_change('range', update)

end = dataframe['time'].max()
slider_time = RangeSlider(start=0, end=end, range=(0, end), step=end//100, title="Time")
slider_time.on_change('range', update)

button_download = Button(label="Download", button_type='success')
button_download.callback = CustomJS(args=dict(source=source), code=open(os.path.join(os.path.dirname(__file__), "download.js")).read())

widgets = [select_concurrency, checkbox_database, multiselect_query_id, slider_rows,
           slider_time, button_download]

#  Layout
curdoc().title = "Big Data Benchmarking"
curdoc().add_root(column(row(widgetbox(widgets), widgetbox(data_table)),
                         row(bar_concurrency_by_database),
                         row([item for item in plots['scatter_plot']]),
                         row([item for item in plots['bar_database_by_query_category']]),
                         row([item for item in plots['bar_database_by_table_size']]),
                         row([item for item in plots['bar_query_id_by_database']]),
                         row([item for item in plots['line_query_id_by_database']]),
                         column([item for item in plots['html_avg_query_time']])))
                         #column([item for sublist in zip(plots['div_avg_query_time'], plots['datatable_avg_query_time']) for item in sublist])))
