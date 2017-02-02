# big data benchmarking


This repo is designed to benchmark various database and big data technologies.

By building a consistent and reproducable environment, you will get consistent results from the benchmarks.

The results of the benchmarks are simply a CSV file that can .  Use the bokeh-server repo which has been designed to plot these results into an interactive dashboard.


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

## Prerequisites

* [Vagrant] - A tool for building complete development environments.
* [VirtualBox] - A cross-platform virtualization application
* Python 3.x
* [Pandas] v0.19.2 - Open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for the Python programming language.
* [Bokeh] v0.12.4 - Python interactive visualization library that targets modern web browsers for presentation.

## Installation
Download and install [Vagrant] and [VirtualBox]

Launch the VM
```
vagrant up
```

SSH into the VM
```
vagrant ssh
```

Navigate to the project directory
```
cd /bokeh-server
```

Start the bokeh server
```
bokeh serve app
```

Open your browser and navigate to:  http://localhost:5006

Terminate the VM
```
vagrant destroy
```

## Authors

* **Justin Naldzin** - [PurpleBooth](https://github.com/PurpleBooth)

[//]: #
[Vagrant]: <https://www.vagrantup.com/>
[VirtualBox]: <https://www.virtualbox.org/>
[Pandas]: <http://pandas.pydata.org/>
[Bokeh]: <http://bokeh.pydata.org/>
[big-data-benchmarking]: <http://github.com/justinnaldzin/big-data-benchmarking>
