#
# cloudlet infrastructure for mobile computing
#
#   author: kiryong ha <krha@cmu.edu>
#
#   copyright (c) 2011-2013 carnegie mellon university
#   licensed under the apache license, version 2.0 (the "license");
#   you may not use this file except in compliance with the license.
#   you may obtain a copy of the license at
#
#       http://www.apache.org/licenses/license-2.0
#
#   unless required by applicable law or agreed to in writing, software
#   distributed under the license is distributed on an "as is" basis,
#   without warranties or conditions of any kind, either express or implied.
#   see the license for the specific language governing permissions and
#   limitations under the license.
#

import os
import sys
if os.path.exists("./elijah"):
    sys.path.insert(0, "./elijah/")

import urllib
from pwd import getpwnam
from provisioning.configuration import Const

from distutils.core import setup
from Cython.Build import cythonize



def download_dependency(download_dir):
    URL_MODIFIED_QEMU = "https://github.com/cmusatyalab/elijah-qemu/releases/download/cloudlet-v0.9.3/qemu-system-x86_64"

    msg = "  " + "-" * 70+ "\n"
    msg += "  Download modified QEMU from %s\n" % URL_MODIFIED_QEMU
    msg += "  It enables on-demand VM fetching. Unlike the codes in elijah-provisioning,\n"
    msg += "  Modified QEMU is distributed under GPL license. Agree (Y/n) ? "
    user_input = raw_input(msg).strip()
    if user_input.lower() == "n":
        sys.exit(1)

    filename = os.path.basename(Const.QEMU_BIN_PATH)
    download_path = os.path.join(download_dir, filename)

    # download binary for modified QEMU
    urllib.urlretrieve(URL_MODIFIED_QEMU, download_path)

    username = os.getenv("SUDO_USER") or os.getenv("USER")
    userinfo = getpwnam(username)
    os.chmod(download_path, 0775)
    os.chown(download_path, userinfo.pw_uid, userinfo.pw_gid)


def get_all_files(package_dir, target_path, exclude_names=list()):
    data_files = list()
    cur_dir = os.path.abspath(os.curdir)
    os.chdir(package_dir)
    for (dirpath, dirnames, filenames) in os.walk(target_path):
        for filename in filenames:
            if filename.startswith('.') is True:
                continue
            if filename in exclude_names:
                continue
            data_files.append(os.path.join(dirpath, filename))
    os.chdir(cur_dir)
    return data_files


download_dependency('elijah/provisioning/lib/bin/x86_64')
script_files = get_all_files(".", "bin")
executable_files = get_all_files('.', 'elijah/provisioning/lib')
conf_files = get_all_files('.', 'elijah/provisioning/config',
                           exclude_names=['cloudlet.db'])

setup(
    name='elijah-provisioning',
    version=str(Const.VERSION),
    description='Cloudlet provisioning library using VM synthesis',
    long_description=open('README.md', 'r').read(),
    url='https://github.com/cmusatyalab/elijah-provisioning/',

    author='Kiryong Ha',
    author_email='krha@cmu.edu',
    keywords="cloud cloudlet provisioning cmu VM libvirt KVM QEMU virtualization",
    license='Apache License Version 2.0',
    scripts=script_files+executable_files,
    packages=[
        'elijah',
        'elijah.provisioning',
        'elijah.provisioning.db',
    ],
    data_files=[
        (Const.CONFIGURATION_DIR, conf_files),
    ],
    requires=[
        'pyliblzma(>=0.5.3)',
        # due to openstack. OpenStack Grizzly is not 
        # compatible with latest version of sqlalchemy
        'sqlalchemy(==0.7.2)',
    ],
    ext_modules = cythonize(["elijah/provisioning/cython_xor.pyx"]),
    classifier=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
    ]
)
