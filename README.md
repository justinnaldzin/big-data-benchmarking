## Overview

This is a portable, reproducible, and completely automated application designed to benchmark various database platforms and big data technologies with a goal of comparing how each performs when working with large datasets.  By ensuring the same dataset is inserted into each database platform and executing syntactically equivalent queries, we can achieve an accurate comparison and understand where one outperforms the other.  An interactive HTTP dashboard containing various charting and graphing elements are used to display the benchmarking results for visual analysis.

<p align="center">
<img src="images/benchmarking.jpg" alt="Benchmarking">
<img src="images/big-data.jpg" alt="Big Data">
</p>

## Audience

When it comes to Big Data and the platforms that house the data, the most common question being asked is:

> "Which big data platform performs best?"

The answer really depends on a few key factors:
- Raw data size (small data vs. big data)
- Query complexity (joins, unions, aggregate and analytical queries)
- Number of concurrent users
- Memory, CPU, and network configurations

And since each environment is different, the more appropriate question to ask is:

> "Which performs best under each scenario?"

<span style="color:#428bca">**If your organization is running multiple Big Data platforms, this benchmarking application is designed to compare them side by side.**</span>

# PLACE DASHBOARD IMAGE HERE

## Features
- Supports the following big data technologies:
 - Oracle Database
 - Oracle Database In-Memory
 - Microsoft SQL Server
 - SAP HANA
 - coming soon:
   - Apache Hive
   - Apache Spark SQL
   - SQLite (use pysqlite)
   - MySQL (use mysqldb or mysql.connector or pymysql)
   - MariaDB (use mysqldb or mysql.connector or pymysql)
   - PostgreSQL (use psycopg2)
   - Microsoft Azure
- Compatible on Linux, macOS, and Windows in a reproducible, portable development environment
- Able to accept multiple source datasets in CSV file format (containing headers)
- Create tables and insert into database the datasets specified
- Run benchmarks on these new datasets or on existing tables in the database
- Able to specify the name of the tables to benchmark
- Able to specify the number of benchmarking iterations to perform
- Able to specify the number of concurrent users to simultaneously perform the same benchmarking queries
- Predefined query templates used to formulate a dynamically generated query which is transferrable between each database technology using a syntactically equivalent query
- Limit query results to a specified row count
- Drop tables (only on the newly created tables) immediately after the benchmark
- Output benchmarking results to CSV
- Plot the results in an interactive HTML dashboard containing data tables, charts and graphs using a [Bokeh](http://bokeh.pydata.org/) server web app

## Requirements

- [Vagrant](https://www.vagrantup.com/) - A tool for building complete development environments
- [VirtualBox](https://www.virtualbox.org/) - A cross-platform virtualization application
- Read/Write permissions to one or more of the following database technologies:
 - Oracle Database
 - Oracle Database In-Memory
 - Microsoft SQL Server
 - SAP HANA
 - more coming soon...
- One or more CSV datasets (containing headers).  Recommended sites to find open datasets:
 - [data.gov](https://www.data.gov) - The home of the U.S. Government's open data
 - [/r/datasets](https://www.reddit.com/r/datasets/) - subreddit with hundreds of interesting datasets
 - [Awesome Public Datasets](https://github.com/caesar0301/awesome-public-datasets) - list of datasets, hosted on GitHub.
 - [Kaggle](https://www.kaggle.com/datasets) - Kaggle datasets

## Installation

Download and install the latest versions of:
- [Vagrant](https://www.vagrantup.com/docs/installation/)
- [VirtualBox](https://www.virtualbox.org/)

Install the Vagrant plugin which automatically installs the host's VirtualBox Guest Additions on the guest system
```sh
vagrant plugin install vagrant-vbguest
```

Clone the repo and navigate into the project directory
```sh
git clone https://github.com/justinnaldzin/big-data-benchmarking.git
cd big-data-benchmarking
```

The folder structure looks like the following:
```
$ tree -L 1
.
├── LICENSE.md
├── README.md
├── app
├── benchmark.py
├── big_data_benchmarking.py
├── config.json.example
├── create_tables.py
├── csv
├── data
├── drop_tables.py
├── log
├── queries
└── vagrant

6 directories, 7 files
```

Move all source CSV datasets into the `big-data-benchmarking/data/` path
```sh
mv /path/to/datasets/*.csv data/
```

Using the example configuration file, create a JSON file named `config.json` and add database credentials to the `connection_string` parameter
```sh
cp config.json.example config.json
vi config.json
```
```json
{
    "Oracle Database":{
        "connection_string":"oracle+cx_oracle://user:password@host"
        ...
    },
    "Oracle Database In-Memory":{
        "connection_string":"oracle+cx_oracle://user:password@host"
        ...
    },
    "SQL Server":{
        "connection_string":"mssql+pymssql://user:password@host"
        ...
    },
    "HANA":{
        "connection_string":"hana://user:password@host:30015"
        ...
    },
    "Hive":{
        "connection_string":"hive://user:password@host:10000/database"
        ...
    }
}
```

Build the environment in a virtual machine
```sh
cd vagrant
vagrant up
```

After provisioning, SSH into the VM
```sh
vagrant ssh
```

Navigate to the project directory on the VM
```sh
cd /big-data-benchmarking
```

Print the command line help menu to view all the options for running the application
```
$ ./big_data_benchmarking.py --help
usage: big_data_benchmarking.py [-h] [-t TABLE_LIKE] [-r ROWS] [-i ITERATIONS]
                                [-u CONCURRENT_USERS] [-p DATA_PATH] [-c] [-d]
                                [database_list [database_list ...]]

Big Data Benchmarking

positional arguments:
  database_list         Specify the list of Databases to benchmark. These
                        names must match the names pre-configured in the
                        'config.json' file.

optional arguments:
  -h, --help            show this help message and exit
  -t TABLE_LIKE, --table-like TABLE_LIKE
                        Specify the name of the tables to benchmark. This uses
                        the SQL 'LIKE' operator to search a specified pattern
                        so use the '%' sign to define wildcards. Default is
                        '%' which will find all existing tables in the
                        database.
  -r ROWS, --rows ROWS  The maximum number of rows to return from each query
                        execution. Default is 10000
  -i ITERATIONS, --iterations ITERATIONS
                        The number of benchmark iterations to perform on the
                        database. Default is 1
  -u CONCURRENT_USERS, --users CONCURRENT_USERS
                        The number of concurrent users to connect to the
                        database. Default is 1
  -p DATA_PATH, --path DATA_PATH
                        Full directory path to where the data files are
                        stored. These will be used to create the tables and
                        insert into database. Default path is: /data
  -c, --create-tables   Create tables and insert into database the data files
                        that exist within in the folder '--path' argument. Not
                        specifying this option will run benchmarks on all
                        existing tables in the database.
  -d, --drop-tables     The '--create-tables' argument must be specified. Only
                        those tables created will be dropped.
```

## Benchmarking

<div class="alert alert-info">
  <strong>Note!</strong> If you simply want to view the interactive dashboard using sample benchmarking results, skip this section and jump down to the [Benchmarking Results](#benchmarking-results) section.
</div>

The `big-data-benchmarking.py` Python script is the main entrypoint to the application.  Here are some example benchmarking scenarios, and how to execute the script:

- Run against **Oracle Database** and **SQL Server** with default options
```sh
./big-data-benchmarking.py "Oracle Database" "SQL Server"
```

- Run on **Oracle Database In-Memory** against all tables whose name starts with "DEV"
```sh
./big-data-benchmarking.py "Oracle Database In-Memory" -t "DEV%"
```

- Run against **HANA** limiting the number of rows to return from each query to **5000**
```sh
./big-data-benchmarking.py "HANA" -r 5000
```

- Run with **50** concurrent users
```sh
./big-data-benchmarking.py "Oracle Database" -u 50
```

- Run **10** iterations
```sh
./big-data-benchmarking.py "Oracle Database" -i 10
```

- Create tables on the database using all CSV datasets in the default `/big-data-benchmarking/data/` path
```sh
./big-data-benchmarking.py "Oracle Database" -c
```

- Create tables specifying a different directory than the default path where the CSV datasets reside
```sh
./big-data-benchmarking.py "Oracle Database" -c -p /some/other/path/
```

- Create tables and drop only those tables after the benchmark completes
```sh
./big-data-benchmarking.py "Oracle Database" -c -d
```

## Benchmarking Results

The `/big-data-benchmarking/csv/` is the location where the actual benchmarking results are written to.  Within this folder you will also find an example benchmarking results CSV file.  This example allows full functionality of the Bokeh server web application even without running any benchmarks.

Here is a sample of what the CSV data looks like:

category|concurrency_factor|database|name|query_executed |query_id|query_template|rows|table_name |table_row_count|table_size_category|time
-|-|-|-|-|-|-|-|-|-|-|-
Aggregate|1|Oracle Database|COUNT * |SELECT COUNT(*) FROM "On_Time_Performance_ALL"|1|SELECT COUNT(*) FROM {table}|1|On_Time_Performance_ALL|160690563|X-Large|149.38998920100005
Aggregate|1|Oracle Database|COUNT|SELECT COUNT("LATE_AIRCRAFT_DELAY") FROM "On_Time_Performance_ALL" |2|SELECT COUNT({numeric_column}) FROM {table}|1|On_Time_Performance_ALL|160690563|X-Large|144.099790143
Aggregate|1|Oracle Database|COUNT DISTINCT|SELECT COUNT(DISTINCT "DEST_CITY_MARKET_ID") FROM "On_Time_Performance_ALL"|3|SELECT COUNT(DISTINCT {numeric_column}) FROM {table}|1|On_Time_Performance_ALL|160690563|X-Large|156.50899973100002
Aggregate|1|Oracle Database|SUM |SELECT SUM("WHEELS_OFF") FROM "On_Time_Performance_ALL"|4|SELECT SUM({numeric_column}) FROM {table}|1|On_Time_Performance_ALL|160690563|X-Large|156.93902191200004
Aggregate|1|Oracle Database|SUM DISTINCT|SELECT SUM(DISTINCT "LATE_AIRCRAFT_DELAY") FROM "On_Time_Performance_ALL"|5|SELECT SUM(DISTINCT {numeric_column}) FROM {table}|1|On_Time_Performance_ALL|160690563|X-Large|149.11920515500003


## Bokeh Server Web App

[Bokeh](http://bokeh.pydata.org/) is a Python interactive visualization library that targets modern web browsers for presentation.  The Bokeh server web app uses the data from the CSV and generates an interactive HTML dashboard consisting of data tables, charts and graphs.  This allows us to visually analyze the benchmarking results and compare each database platform.

Instruct Bokeh server to launch the web app
```sh
bokeh serve app
```

Then open your browser and navigate to the dashboard:  http://localhost:5006

# DASHBOARD IMAGE HERE

## Conclusion

Finally, when you are finished running the benchmarks and no longer need to view the dashboard results, terminate the VM
```sh
vagrant destroy
```
<div class="alert alert-info">
  <strong>Note!</strong> The `big-data-benchmarking` project folder remains on your local system, along with the CSV benchmarking results.  You will no longer be able to run the benchmarks or view the dashboard once the VM is terminated.  You can easily reprovision the VM and start over
</div>
```sh
vagrant up
```

## Feature Requests

- Option to specify the *minimum* amount of rows to return from each query.  Currently you are only permitted to limit the *maximum* amount of rows
- Benchmark different python drivers against the same database type (MySQL example: `mysqldb` vs `mysql.connector` vs `pymysql`)
- Embed Bokeh into Flask app with Jinja templates

## Author

Justin Naldzin - [GitHub](https://github.com/justinnaldzin)
