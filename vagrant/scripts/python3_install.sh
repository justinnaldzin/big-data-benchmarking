#!/bin/sh -e

set -x

# Install Python 3
yum -y update
yum -y install https://centos7.iuscommunity.org/ius-release.rpm   # Inline with Upstream Stable. A community project, IUS provides Red Hat Package Manager (RPM) packages for some newer versions of select software.
yum -y install python35u-3.5.2  # Python 3.5.2
yum -y install python35u-pip  # pip
yum -y install python35u-devel  # libraries and header files
sudo ln -s /usr/bin/python3.5 /usr/bin/python3  # create a symbolic link "python3" to point to "python3.5"
sudo ln -s /usr/bin/pip3.5 /usr/bin/pip3  # create a symbolic link "pip3" to point to "pip3.5"

# Install Oracle Instant Client and cx_Oracle
rpm -ivh /vagrant/installs/oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - Basic
rpm -ivh /vagrant/installs/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - SDK
rpm -ivh /vagrant/installs/oracle-instantclient12.1-odbc-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - ODBC
echo "/usr/lib/oracle/12.1/client64/lib/" > /etc/ld.so.conf.d/oracle.conf  # add Oracle client to library path
ldconfig  # enable the shared library path system wide

# Add oracle library paths to bash profile for 'root' user
echo 'export ORACLE_VERSION="12.1"' >> $HOME/.bashrc
echo 'export ORACLE_HOME="/usr/lib/oracle/$ORACLE_VERSION/client64"' >> $HOME/.bashrc
echo 'export PATH=$PATH:"$ORACLE_HOME/bin"' >> $HOME/.bashrc
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"$ORACLE_HOME/lib"' >> $HOME/.bashrc
. $HOME/.bashrc
mkdir -p $ORACLE_HOME/network/admin/
cp /benchmarking/vagrant/installs/tnsnames.ora $ORACLE_HOME/network/admin/

# Add oracle library paths to bash profile for 'vagrant' user
echo 'export ORACLE_VERSION="12.1"' >> /home/vagrant/.bashrc
echo 'export ORACLE_HOME="/usr/lib/oracle/$ORACLE_VERSION/client64"' >> /home/vagrant/.bashrc
echo 'export PATH=$PATH:"$ORACLE_HOME/bin"' >> /home/vagrant/.bashrc
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"$ORACLE_HOME/lib"' >> /home/vagrant/.bashrc

# Install additional Python libraries
pip3 install --upgrade pip setuptools wheel virtualenv
pip3 install requests pandas bokeh inflection
pip3 install cx_Oracle  # Python interface to Oracle
pip3 install pymssql  # DB-API interface to Microsoft SQL Server for Python.
pip3 install pyhdb  # SAP HANA Database Client for Python
pip3 install pyhive  # Python interface to Hive
pip3 install csvkit  # Suite of utilities for converting to and working with CSV.
#pip install ??????  # Spark SQL

