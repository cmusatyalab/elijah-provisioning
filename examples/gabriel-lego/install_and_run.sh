#! /bin/bash

set -ex

function die { echo $1; exit 42; }

sudo apt-get update \
&& sudo apt-get upgrade -y \
&& sudo apt-get install -y \
            gcc \
            git \
            python-dev \
            default-jre \
            python-pip \
            pssh \
            python-psutil

git clone https://github.com/cmusatyalab/gabriel.git $HOME/gabriel
sudo pip install -r $HOME/gabriel/server/requirements.txt

sudo apt-get -y install python-opencv
wget -O $HOME/gabriel-apps-lego.tar.gz https://www.dropbox.com/s/8032yj3s867zjvx/gabriel-apps-lego.tar.gz
tar -xf gabriel-apps-lego.tar.gz

echo "install finished! launching lego..."
cd $HOME/gabriel/server/bin
./gabriel-control -l &
sleep 2
./gabriel-ucomm -s 127.0.0.1:8021 &
sleep 2
cd $HOME/gabriel-apps/lego
./proxy.py -s 127.0.0.1:8021 &
