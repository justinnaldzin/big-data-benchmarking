
#!/bin/sh -e

set -x

#Install Java JDK
rpm -ivh /vagrant/installs/jdk-8u111-linux-x64.rpm

# Install Apache Spark
tar zxvf /vagrant/installs/spark-2.0.2-bin-hadoop2.7.tgz -C /opt
mv /opt/spark-2.0.2-bin-hadoop2.7 /opt/spark-2.0.2
ln -s /opt/spark-2.0.2 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> $HOME/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> $HOME/.bashrc

# Minimize the Verbosity of Spark
cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
# replace INFO with WARN at every line


# PySpark
# By default, PySpark requires python to be available on the system PATH and use it to run programs; an alternate 
# Python executable may be specified by setting the PYSPARK_PYTHON environment variable in conf/spark-env.sh

# Standalone PySpark applications should be run using the bin/pyspark script, which automatically configures the 
# Java and Python environment using the settings in conf/spark-env.sh. The script automatically adds the 
# bin/pyspark package to the PYTHONPATH.

# Run pySpark interactively
#./bin/pyspark
#words = sc.textFile("/usr/share/dict/words")
#words.filter(lambda w: w.startswith("spar")).take(5)
#help(pyspark) # Show all pyspark functions

#To connect to a non-local cluster, or use multiple cores, set the MASTER environment variable. For example
#MASTER=spark://IP:PORT ./bin/pyspark

# Or, to use four cores on the local machine:
#MASTER=local[4] ./bin/pyspark

# This is the only thing that works!
# export SPARK_LOCAL_IP=127.0.0.1  # bind Spark to local IP


# PySpark examples
from pyspark import SparkContext
sc = SparkContext("local", "App Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])

from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)





# Jupyter Notebook with Apache Spark
yum -y install tkinter
yum -y install nano centos-release-scl zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libpng-devel libjpg-devel atlas-devel
pip3 install numpy scipy pandas scikit-learn pyzmq matplotlib
pip3 install jinja2 --upgrade
pip3 install jupyter
jupyter profile create pyspark

jupyter profile create spark



jupyter notebook --no-browser --port=8888
# shell script to start Jupyter
cat <<EOF >> ~/start_jupyter_notebook.sh
#!/bin/bash
source /opt/rh/python27/enable

EOF
chmod +x ~/start_jupyter_notebook.sh

# Port Forwarding
# need to forward port 8889 from guest VM to host

~/start_jupyter_notebook.sh


