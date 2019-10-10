from __future__ import with_statement

import os
import sys
import platform

from fabric.api import env
from fabric.api import hide
from fabric.api import run
from fabric.api import local
from fabric.api import sudo
from fabric.api import task
from fabric.api import abort
from fabric.api import puts
from fabric.contrib.files import comment,append,upload_template
from fabric.context_managers import cd
from fabric.context_managers import settings
from distutils.version import LooseVersion
import urllib



# install on local machine
env.user = 'root'
env.warn_only = True
env.hosts = ['localhost']
dist = None

def check_system_support():
    # check hardware support
    if local("egrep '^flags.*(vmx|svm)' /proc/cpuinfo > /dev/null").failed:
        abort("Virtual machine extensions (vmx) instructions set are required.")

    # check minimum kernel version:
    #   Linux kernel under 3.13.0 has a bug where the kernel crash 
    #   when EPT support enabled with FUSE+mmap.
    WORKING_KERNEL_VERSION = "3.13.0"
    kernel_version = platform.platform().split("-")[1]
    if LooseVersion(kernel_version) < LooseVersion(WORKING_KERNEL_VERSION):
        msg = "Linux Kernel lower than %s has a bug when using FUSE + mmap"\
            % WORKING_KERNEL_VERSION
        abort(msg)

    # check OS version
    cmd = "cat /etc/lsb-release | grep DISTRIB_CODENAME | awk -F'=' '{print $2}'"

    dist = local(cmd, capture=True)
    if dist != 'bionic' and dist != 'xenial':
        msg = "nephele is only supported on Ubuntu Bionic (18.04) or Xenial (16.04)"
        abort(msg)
    return dist


def package_installed(pkg_name):
    """ref: http:superuser.com/questions/427318/#comment490784_427339"""
    cmd_f = 'dpkg-query -l "%s" | grep -q ^.i'
    cmd = cmd_f % (pkg_name)
    with settings(warn_only=True):
        result = local(cmd,capture=True)
        return result.succeeded


def yes_install(pkg_name):
    """ref: http://stackoverflow.com/a/10439058/1093087"""
    local('apt-get --force-yes -yes install %s' % (pkg_name))

def download_dependency(download_dir):
    URL_MODIFIED_QEMU = "https://owncloud.cmusatyalab.org/owncloud/index.php/s/viEQ29YYOMlkgJm/download"

    download_path = os.path.join(download_dir, 'nephele-qemu.tar.gz')

    # download binary for modified QEMU
    urllib.urlretrieve(URL_MODIFIED_QEMU, download_path)

    local("tar -xvf %s" % download_path)
    local("dpkg -i qemu*.deb",shell='/bin/bash')

@task
def update():
    #only refresh the python files
    current_dir = os.path.abspath(os.curdir)
    with cd(current_dir):
        # remove previous build directory
        with settings(hide('everything')):
            local("rm -rf ./build")
            local("pip uninstall --y elijah-provisioning")
        # install python package
        if local("python setup.py install",capture=True).failed:
            abort("cannot install cloudlet library")
        # clean-up
        with settings(hide('everything')):
            local("rm -rf ./build")

@task
def install():
    current_dir = os.path.abspath(os.curdir)

    # check hardware support
    dist = check_system_support()

    # install dependent packages
    with settings(hide('stdout'), warn_only=True):
        cmd = "apt-get update"
        local(cmd)
    with settings(hide('running'), warn_only=True):
        cmd = "apt-get install --force-yes -y qemu-kvm libvirt-bin libglu1-mesa "
        cmd += "gvncviewer python-dev python-libvirt python-lxml python-lzma "
        cmd += "apparmor-utils libc6-i386 python-pip libxml2-dev libxslt1-dev xdelta3 apache2"
        if local(cmd, capture=True).failed:
            abort("Failed to install libraries")

        # Python-xdelta3 is no longer supported
        # But you can install deb
        with cd(current_dir):
            package_name = "python-xdelta3.deb"
            cmd = "wget http://mirrors.kernel.org/ubuntu/pool/universe/x/xdelta3/python-xdelta3_3.0.0.dfsg-1build1_amd64.deb -O %s" % package_name
            if local(cmd, capture=True).failed:
                abort("Failed to download %s" % package_name)
            if local("dpkg -i %s" % package_name, capture=True).failed:
                abort("Failed to install %s" % package_name)
            local("rm -rf %s" % package_name)

    #download and install modified qemu binaries
    download_dependency(current_dir)

    # install python-packages
    with cd(current_dir):
        if local("pip install -r requirements.txt", capture=True).failed:
            abort("Failed to install python libraries")
    #copy heatmap to webserver directory and set permissions
    if local("mkdir /var/www/html/heatmap",capture=True).failed:
        print("Failed to mkdir /var/www/html/heatmap")
    if local("chmod 664 /var/www/html/heatmap",capture=True).failed:
        print("Failed to set perms for heatmap directory")
    with cd(current_dir):
        local("cp -r heatmap/* /var/www/html/heatmap")
    if not os.path.exists('/var/nephele/images'):
        if local("mkdir -p /var/nephele/images",capture=True).failed:
            print("Failed to create nephele image directory!")

    # check bios.bin file
    bios_files = ["/usr/share/qemu/bios.bin",
                  "/usr/share/qemu/vgabios-cirrus.bin"]
    for bios_file in bios_files:
        if not os.path.exists(bios_file):
            filename = os.path.basename(bios_file)
            local("ln -s /usr/share/seabios/%s %s" % (filename, bios_file))

    # disable libvirtd from appArmor to enable custom KVM
    if local("aa-complain /usr/sbin/libvirtd",capture=True).failed:
        abort("Failed to disable AppArmor for custom KVM")

    # add current user to groups (optional)
    username = env.get('user')
    if local("adduser %s kvm" % username, capture=True).failed:
        abort("Cannot add user to kvm group")
    if dist == 'bionic':
        grp = 'libvirt'
    else:
        grp = 'libvirtd'
    cmd = "adduser %s %s" % (username, grp)
    if local(cmd,capture=True).failed:
        abort("Cannot add user to libvirt group")

    #Comment out the following deny rules for apparmor so that we can run in the qemu:///system space
    #upload_template('TEMPLATE.qemu','/etc/apparmor.d/libvirt/TEMPLATE.qemu')
    local("cp TEMPLATE.qemu /etc/apparmor.d/libvirt/TEMPLATE.qemu")
    local("cat libvirt-qemu-abstractions >> /etc/apparmor.d/abstractions/libvirt-qemu")

    # Check fuse support:
    #   qemu-kvm changes the permission of /dev/fuse, so we revert back the
    #   permission. This bug is fixed from udev-175-0ubuntu26
    #   Please see https://bugs.launchpad.net/ubuntu/+source/udev/+bug/1152718
    if local("chmod 1666 /dev/fuse",capture=True).failed:
        abort("Failed to enable fuse for the user")
    if local("chmod 644 /etc/fuse.conf",capture=True).failed:
        abort("Failed to change permission of fuse configuration")
    if local("sed -i 's/#user_allow_other/user_allow_other/g' /etc/fuse.conf",capture=True).failed:
        abort("Failed to allow other user to access FUSE file")

    # install cloudlet package
    with cd(current_dir):
        # remove previous build directory
        with settings(hide('everything')):
            local("rm -rf ./build")
            local("pip uninstall --y elijah-provisioning")
        # install python package
        if local("python setup.py install",capture=True).failed:
            abort("cannot install cloudlet library")
        # clean-up
        with settings(hide('everything')):
            local("rm -rf ./build")

    sys.stdout.write("[SUCCESS] nephele is installed\n")

