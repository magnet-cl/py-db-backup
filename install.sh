#!/usr/bin/env bash

# install pip
sudo apt-get install -y python-pip

# update pip
sudo -H pip install -U pip

# required packages
sudo -H pip install -U boto
sudo -H pip install -U sh

# initial config file
cp config.ini.default config.ini
