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
from fabric.contrib.files import comment,append
from fabric.context_managers import cd
from fabric.context_managers import settings
from distutils.version import LooseVersion



# install on local machine
env.run = local
env.warn_only = True
env.hosts = ['localhost']
os_dist = None


def check_system_support():
    # check hardware support
    if run("egrep '^flags.*(vmx|svm)' /proc/cpuinfo > /dev/null").failed:
        abort("Need hardware VM support (vmx)")

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
    with settings(hide('everything'), warn_only=True):
        os_dist = run(cmd)
        if os_dist != 'bionic' and os_dist != "xenial":
            msg = "Supported only on Ubuntu Bionic (18.04) or Xenial (16.04)"
            abort(msg)
        return os_dist
    return None


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


@task
def install():
    current_dir = os.path.abspath(os.curdir)

    # check hardware support
    dist = check_system_support()

    # install dependent packages
    with settings(hide('stdout'), warn_only=True):
        cmd = "sudo apt-get update"
        sudo(cmd)
    with settings(hide('running'), warn_only=True):
        cmd = "apt-get install --force-yes -y qemu-kvm libvirt-bin libglu1-mesa "
        cmd += "gvncviewer python-dev python-libvirt python-lxml python-lzma "
        cmd += "apparmor-utils libc6-i386 python-pip libxml2-dev libxslt1-dev apache2"
        if dist == "precise":
            cmd += " python-xdelta3"
            if sudo(cmd).failed:
                abort("Failed to install libraries")
        else:
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
            abort("Failed to install python libraries")
    #copy heatmap to webserver directory and set permissions
    if sudo("mkdir /var/www/html/heatmap").failed:
        print("Failed to mkdir /var/www/html/heatmap")
    if sudo("chmod 777 /var/www/html/heatmap").failed:
        print("Failed to set perms for heatmap directory")
    with cd(current_dir):
        sudo("cp -r heatmap/* /var/www/html/heatmap")

    # check bios.bin file
    bios_files = ["/usr/share/qemu/bios.bin",
                  "/usr/share/qemu/vgabios-cirrus.bin"]
    for bios_file in bios_files:
        if not os.path.exists(bios_file):
            filename = os.path.basename(bios_file)
            sudo("ln -s /usr/share/seabios/%s %s" % (filename, bios_file))

    # disable libvirtd from appArmor to enable custom KVM
    if sudo("aa-complain /usr/sbin/libvirtd").failed:
        abort("Failed to disable AppArmor for custom KVM")

    # add current user to groups (optional)
    username = env.get('user')
    if sudo("adduser %s kvm" % username).failed:
        abort("Cannot add user to kvm group")
    if os_dist == 'bionic':
        grp = 'libvirt'
    else:
        grp = 'libvirtd'
    if sudo("adduser %s %s" % (username, grp)).failed:
        abort("Cannot add user to libvirt group")

    #Comment out the following deny rules for apparmor so that we can run in the qemu:///system space
    comment('/etc/apparmor.d/abstractions/libvirt-qemu', 'deny /tmp/', use_sudo=True)
    comment('/etc/apparmor.d/abstractions/libvirt-qemu', 'deny /var/tmp/', use_sudo=True)
    append('/etc/apparmor.d/abstractions/libvirt-qemu', '/tmp/cloudlet-*/** rw,', use_sudo=True)
    append('/etc/apparmor.d/abstractions/libvirt-qemu', '/tmp/qemu_debug_messages rw,', use_sudo=True)
    append('/etc/apparmor.d/abstractions/libvirt-qemu', '/tmp/qemu* rw,', use_sudo=True)  

    # Check fuse support:
    #   qemu-kvm changes the permission of /dev/fuse, so we revert back the
    #   permission. This bug is fixed from udev-175-0ubuntu26
    #   Please see https://bugs.launchpad.net/ubuntu/+source/udev/+bug/1152718
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
        # install python package
        if sudo("python setup.py install").failed:
            abort("cannot install cloudlet library")
        # clean-up
        with settings(hide('everything')):
            sudo("rm -rf ./build")

    sys.stdout.write("[SUCCESS] VM synthesis code is installed\n")

