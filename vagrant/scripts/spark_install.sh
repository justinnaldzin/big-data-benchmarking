
#!/bin/sh -e

set -x

#Install Java JDK
rpm -ivh /vagrant/installs/jdk-8u111-linux-x64.rpm

# Install Apache Spark
tar zxvf /vagrant/installs/spark-2.0.2-bin-hadoop2.7.tgz -C /opt
mv /opt/spark-2.0.2-bin-hadoop2.7 /opt/spark-2.0.2
ln -s /opt/spark-2.0.2 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> /etc/profile.d/spark.sh
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> /etc/profile.d/spark.sh
source /etc/profile.d/spark.sh

# Minimize the Verbosity of Spark
cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties

# Spark environment variables are sourced from: $SPARK_HOME/conf/spark-env.sh
export PYSPARK_PYTHON=python3  # Instruct PySpark to use an alternate Python executable


# Jupyter Notebook with Apache Spark
yum -y install tkinter
yum -y install nano centos-release-scl zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libpng-devel libjpg-devel atlas-devel
pip3 install numpy scipy pandas scikit-learn pyzmq matplotlib
pip3 install jinja2 --upgrade
pip3 install jupyter

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


