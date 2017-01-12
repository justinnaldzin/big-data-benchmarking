#!/bin/sh -e

set -x

# Base Install
yum -y update
yum -y install yum-utils
yum -y install git curl wget epel-release gcc
yum -y groupinstall development
yum -y install python-pip python-devel python-setuptools python-virtualenv libffi-devel openssl-devel
pip install --upgrade pip setuptools wheel virtualenv
