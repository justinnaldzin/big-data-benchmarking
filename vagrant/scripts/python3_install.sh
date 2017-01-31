#!/bin/sh -e

set -x

# Install Python 3
yum -y update
yum -y install https://centos7.iuscommunity.org/ius-release.rpm   # Inline with Upstream Stable. A community project, IUS provides Red Hat Package Manager (RPM) packages for some newer versions of select software.
yum -y install python35u  # Python 3.5
yum -y install python35u-pip  # pip
yum -y install python35u-devel  # libraries and header files
yum -y install python-tools  # 2to3 tool
yum -y install cyrus-sasl-devel
sudo ln -s /usr/bin/python3.5 /usr/bin/python3  # create a symbolic link "python3" to point to "python3.5"
sudo ln -s /usr/bin/pip3.5 /usr/bin/pip3  # create a symbolic link "pip3" to point to "pip3.5"

# Install Oracle Instant Client and cx_Oracle
rpm -ivh /vagrant/installs/oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - Basic
rpm -ivh /vagrant/installs/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - SDK
rpm -ivh /vagrant/installs/oracle-instantclient12.1-odbc-12.1.0.2.0-1.x86_64.rpm  # install Instant Client Package - ODBC
echo "/usr/lib/oracle/12.1/client64/lib/" > /etc/ld.so.conf.d/oracle.conf  # add Oracle client to library path
ldconfig  # enable the shared library path system wide

# Add oracle library paths to bash profile for 'root' user
echo 'export ORACLE_VERSION="12.1"' >> /etc/profile.d/oracle.sh
echo 'export ORACLE_HOME="/usr/lib/oracle/$ORACLE_VERSION/client64"' >> /etc/profile.d/oracle.sh
echo 'export PATH=$PATH:"$ORACLE_HOME/bin"' >> /etc/profile.d/oracle.sh
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"$ORACLE_HOME/lib"' >> /etc/profile.d/oracle.sh
source /etc/profile.d/oracle.sh
if [ -e /vagrant/installs/tnsnames.ora ]; then mkdir -p $ORACLE_HOME/network/admin/ && cp /vagrant/installs/tnsnames.ora $ORACLE_HOME/network/admin/; fi

# Install additional Python libraries
pip3 install --upgrade pip setuptools wheel virtualenv
pip3 install -r /vagrant/installs/pip_dependencies.txt
/usr/bin/2to3 -w /usr/lib/python3.5/site-packages/sqlalchemy_hana/  # transform the package from Python 2.x to 3.x
/usr/bin/2to3 -w /usr/lib/python3.5/site-packages/pyhive/