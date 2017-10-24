#!/bin/bash

set -e -x

apt-get -y update
DEBIAN_FRONTEND=noninteractive apt-get -y install \
  git openssh-server qemu-kvm libvirt-bin libglu1-mesa \
  gvncviewer python-dev python-libvirt python-lxml python-lzma \
  apparmor-utils libc6-i386 python-pip libxml2-dev libxslt1-dev
wget http://mirrors.kernel.org/ubuntu/pool/universe/x/xdelta3/python-xdelta3_3.0.0.dfsg-1build1_amd64.deb -O python3-xdelta3.deb
dpkg -i python3-xdelta3.deb

git clone https://github.com/cmusatyalab/elijah-provisioning.git /home/ubuntu/elijah-provisioning
cd /home/ubuntu/elijah-provisioning
git checkout orangebox
chown -R ubuntu:ubuntu /home/ubuntu/elijah-provisioning
pip install -r requirements.txt

chmod 1666 /dev/fuse
chmod 644 /etc/fuse.conf
sed -i 's/#user_allow_other/user_allow_other/g' /etc/fuse.conf

mkdir /usr/libexec
ln -s /usr/share/seabios/bios.bin /usr/share/qemu/bios.bin
ln -s /usr/share/seabios/vgabios-cirrus.bin /usr/share/qemu/vgabios-cirrus.bin
ln -s /usr/lib/qemu/qemu-bridge-helper /usr/libexec/qemu-bridge-helper
chmod u+s /usr/libexec/qemu-bridge-helper
aa-complain /usr/sbin/libvirtd
adduser ubuntu kvm
adduser ubuntu libvirtd

USER=root python setup.py install
