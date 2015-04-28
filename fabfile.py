from __future__ import with_statement

from fabric.api import env
from fabric.api import hide
from fabric.api import run
from fabric.api import local
from fabric.api import sudo
from fabric.api import task
from fabric.api import abort
from fabric.context_managers import cd
from fabric.context_managers import settings

import os
import sys
import platform
from distutils.version import LooseVersion


def check_VM_support():
    if run("egrep '^flags.*(vmx|svm)' /proc/cpuinfo > /dev/null").failed:
        abort("Need hardware VM support (vmx)")


def check_os_distribution():
    cmd = "cat /etc/lsb-release | grep DISTRIB_CODENAME | awk -F'=' '{print $2}'"
    os_dist = ''
    with settings(hide('everything'), warn_only=True):
        os_dist = run(cmd)
    if os_dist != 'precise' and os_dist != "trusty":
        msg = "Support only Ubuntu Precise (12.04) or Ubuntu Trusty (14.04)"
        abort(msg)
    return os_dist


def check_kernel_version():
    """check kernel version
    Kernel has serious bug where the kernel cause crash when EPT support with
    FUSE+mmap.  This bug is fixed since Linux kernel 3.13.0
    """

    WORKING_KERNEL_VERSION = "3.13.0"
    kernel_version = platform.platform().split("-")[1]
    if LooseVersion(kernel_version) < LooseVersion(WORKING_KERNEL_VERSION):
        msg = "Linux Kernel lower than %s has serious bug in using FUSE + mmap"\
            % WORKING_KERNEL_VERSION
        abort(msg)


def package_installed(pkg_name):
    """ref: http:superuser.com/questions/427318/#comment490784_427339"""
    cmd_f = 'dpkg-query -l "%s" | grep -q ^.i'
    cmd = cmd_f % (pkg_name)
    with settings(warn_only=True):
        result = run(cmd)
        return result.succeeded


def yes_install(pkg_name):
    """ref: http://stackoverflow.com/a/10439058/1093087"""
    run('apt-get --force-yes -yes install %s' % (pkg_name))


def disable_EPT():
    # (Optional) disable EPT support
    # When you use EPT support with FUSE+mmap, it randomly causes kernel panic.
    # We're investigating it whether it's Linux kernel bug or not.
    if run("egrep '^flags.*(ept)' /proc/cpuinfo > /dev/null").failed:
        return
    else:
        # disable EPT
        sudo('modprobe -r kvm_intel')
        sudo('modprobe kvm_intel "ept=0"')


@task
def localhost():
    env.run = local
    env.warn_only = True
    env.hosts = ['localhost']


@task
def install():
    current_dir = os.path.abspath(os.curdir)

    # check support
    check_VM_support()
    check_kernel_version()
    dist = check_os_distribution()

    # install dependent packages
    sudo("apt-get update")
    cmd = "apt-get install --force-yes -y qemu-kvm libvirt-bin gvncviewer "
    cmd += "python-dev python-libvirt python-lxml python-lzma "
    cmd += "apparmor-utils libc6-i386 python-pip libxml2-dev libxslt1-dev"
    if dist == "precise":
        cmd += " python-xdelta3"
        if sudo(cmd).failed:
            abort("Failed to install libraries")
    elif dist == "trusty":
        if sudo(cmd).failed:
            abort("Failed to install libraries")
        # Python-xdelta3 is no longer supported in Ubuntu 14.04 LTS.
        # But you can install deb of Ubunutu 12.04 at Ubuntu 14.04.
        with cd(current_dir):
            package_name = "python-xdelta3.deb"
            cmd = "wget http://mirrors.kernel.org/ubuntu/pool/universe/x/xdelta3/python-xdelta3_3.0.0.dfsg-1build1_amd64.deb -O %s" % package_name
            if sudo(cmd).failed:
                abort("Failed to download %s" % package_name)
            if sudo("dpkg -i %s" % package_name).failed:
                abort("Failed to install %s" % package_name)
            sudo("rm -rf %s" % package_name)

    # install python-packages
    with cd(current_dir):
        if sudo("pip install -r requirements.txt").failed:
            # Use old-version of msgpack library due to OpenStack compatibility
            # See at https://bugs.launchpad.net/devstack/+bug/1134575
            abort("Failed to install python libraries")

    # disable libvirtd from appArmor to enable custom KVM
    if sudo("aa-complain /usr/sbin/libvirtd").failed:
        abort("Failed to disable AppArmor for custom KVM")

    # add current user to groups (optional)
    username = env.get('user')
    if sudo("adduser %s kvm" % username).failed:
        abort("Cannot add user to kvm group")
    if sudo("adduser %s libvirtd" % username).failed:
        abort("Cannot add user to libvirtd group")
    if sudo("adduser %s fuse" % username).failed:
        abort("Cannot add user to fuse group")

    # Make sure to have fuse support
    # qemu-kvm changes the permission of /dev/fuse, so we revert back the
    # permission. This bug is fixed from udev-175-0ubuntu26
    # Please see https://bugs.launchpad.net/ubuntu/+source/udev/+bug/1152718
    if sudo("chmod 1666 /dev/fuse").failed:
        abort("Failed to enable fuse for the user")
    if sudo("chmod 644 /etc/fuse.conf").failed:
        abort("Failed to change permission of fuse configuration")
    if sudo("sed -i 's/#user_allow_other/user_allow_other/g' /etc/fuse.conf"):
        abort("Failed to allow other user to access FUSE file")


    # install cloudlet package
    with cd(current_dir):
        # remove previous build directory
        with settings(hide('everything')):
            sudo("rm -rf ./build")
            sudo("pip uninstall --y elijah-provisioning")
        if sudo("python setup.py install").failed:
            abort("cannot install cloudlet library")

    sys.stdout.write("[SUCCESS] VM synthesis code is installed\n")

