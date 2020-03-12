#!/usr/bin/env python
#
# Cloudlet Infrastructure for Mobile Computing
#
#   Author: Kiryong Ha <krha@cmu.edu>
#
#   Copyright (C) 2011-2013 Carnegie Mellon University
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import os
import sys
import pprint


class ConfigurationError(Exception):
    pass


def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return exe_file


class Const(object):
    VERSION = str("0.9.4")
    CLOUDLET_KVM_RELEASE = "https://github.com/cmusatyalab/elijah-qemu/releases"
    HOME_DIR = os.path.abspath(os.path.expanduser("~"))
    CONFIGURATION_DIR = os.path.join('/', 'var', 'lib', 'cloudlet', 'conf')
    QUEUE_SUCCESS_MESSAGE = "!!@#^&!MemorySnapshot Transfer SUCCESS Marker!!@#^&!"
    QUEUE_FAILED_MESSAGE = "!!@#^&!MemorySnapshot Transfer FAILED Marker!!@#^&!"
    QUEUE_SUCCESS_MESSAGE_LEN = len(QUEUE_SUCCESS_MESSAGE)
    QUEUE_FAILED_MESSAGE_LEN = len(QUEUE_FAILED_MESSAGE)

    DIR_NEPHELE_PID = "/var/nephele/pid"

    PRODUCE_HEATMAP_IMAGES = False

    BASE_DISK = ".base-img"
    BASE_MEM = ".base-mem"
    BASE_DISK_META = ".base-img-meta"
    BASE_MEM_META = ".base-mem-meta"
    BASE_HASH_VALUE = ".base-hash"
    OVERLAY_URIs = ".overlay-URIs"
    OVERLAY_META = "overlay-meta"
    OVERLAY_FILE_PREFIX = "overlay-blob"
    OVERLAY_ZIP = "overlay.zip"
    OVERLAY_LOG = ".overlay-log"
    LOG_PATH = "/var/tmp/cloudlet/log-synthesis"
    OVERLAY_BLOB_SIZE_KB = 1024*1024  # 1G

    COMPRESSION_LZMA = 1
    COMPRESSION_BZIP2 = 2
    COMPRESSION_GZIP = 3

    META_BASE_VM_SHA256 = "base_vm_sha256"
    META_RESUME_VM_DISK_SIZE = "resumed_vm_disk_size"
    META_RESUME_VM_MEMORY_SIZE = "resumed_vm_memory_size"
    META_OVERLAY_FILES = "overlay_files"
    META_OVERLAY_FILE_NAME = "overlay_name"
    META_OVERLAY_FILE_COMPRESSION = "overlay_compression"
    META_OVERLAY_FILE_SIZE = "overlay_size"
    META_OVERLAY_FILE_DISK_CHUNKS = "disk_chunk"
    META_OVERLAY_FILE_MEMORY_CHUNKS = "memory_chunk"
    META_VM_TITLE = "title"
    META_FWD_PORTS = "fwd_ports"

    OP_BUILD_IMAGE = "BuildImage"
    OP_BUILD_SNAPSHOT = "BuildSnapshot"
    OP_HANDOFF = "Handoff"
    OP_DELETE_IMAGE = "DeleteImage"
    OP_DELETE_SNAPSHOT = "DeleteSnapshot"
    OP_EXPORT_IMAGE = "ExportImage"
    OP_IMPORT_IMAGE = "ImportImage"

    MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
    QEMU_BIN_PATH = which("qemu-system-x86_64")
    FREE_MEMORY_BIN_PATH = which("cloudlet_free_page_scan")
    CLOUDLETFS_PATH = which("cloudlet_vmnetfs")
    XRAY_BIN_PATH = which("cloudlet_disk_analyzer")

    # personal information
    CLOUDLET_DB = os.path.abspath(
        os.path.join( HOME_DIR, ".cloudlet/config/cloudlet.db"))
    BASE_VM_DIR = os.path.abspath(
        os.path.join( HOME_DIR, ".cloudlet", "baseVM"))

    # global configuration files
    CLOUDLET_DB_SCHEMA = os.path.join(CONFIGURATION_DIR, "schema.sql")
    BASEVM_PACKAGE_SCHEMA = os.path.join(CONFIGURATION_DIR, "package.xsd")
    TEMPLATE_XML = os.path.join(CONFIGURATION_DIR, "VM_TEMPLATE.xml")
    TEMPLATE_OVF = os.path.join(CONFIGURATION_DIR, "ovftransport.iso")
    CHUNK_SIZE = 4096
    LIBVIRT_HEADER_SIZE = CHUNK_SIZE*2

    @staticmethod
    def _check_path(name, path):
        if not os.path.exists(path):
            message = "Cannot find name at %s" % (path)
            raise ConfigurationError(message)
        if not os.access(path, os.R_OK):
            message = "File exists but cannot read the file at %s" % (path)
            raise ConfigurationError(message)

    @staticmethod
    def get_basepath(base_disk_path, check_exist=False):
        Const._check_path('base disk', base_disk_path)

        image_name = os.path.splitext(os.path.basename(base_disk_path))[0]
        dir_path = os.path.dirname(base_disk_path)
        diskmeta = os.path.join(dir_path, image_name+Const.BASE_DISK_META)
        mempath = os.path.join(dir_path, image_name+Const.BASE_MEM)
        memmeta = os.path.join(dir_path, image_name+Const.BASE_MEM_META)

        # check sanity
        if check_exist:
            Const._check_path('base memory', mempath)
            Const._check_path('base disk-hash', diskmeta)
            Const._check_path('base memory-hash', memmeta)

        return diskmeta, mempath, memmeta

    @staticmethod
    def get_base_hashpath(base_disk_path):
        image_name = os.path.splitext(os.path.basename(base_disk_path))[0]
        dir_path = os.path.dirname(base_disk_path)
        return os.path.join(dir_path, image_name+Const.BASE_HASH_VALUE)


class Options(object):

    def __init__(self):
        self.TRIM_SUPPORT = True
        self.FREE_SUPPORT = False
        self.XRAY_SUPPORT = False
        self.DISK_ONLY = False
        self.ZIP_CONTAINER = False

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(dictionary):
        o = Options()
        o.__dict__ = dictionary
        return o


class VMOverlayCreationMode(object):
    PIPE_ONE_ELEMENT_SIZE = 4096*100  # 400KB == Max Pipe size is 1MB
    EMULATED_BANDWIDTH_Mbps = 100000  # Mbps
    # only used for experiement. If it's bigger than 0, adaptation use this
    # value to transmit over the network
    USE_STATIC_NETWORK_BANDWIDTH = -1
    USE_PUBSUB_NETWORK_BANDWIDTH = False
    MEASURE_AVERAGE_TIME = 2  # seconds
    MAX_THREAD_NUM = 4
    HANDOFF_DEST_PORT_DEFAULT = 8022

    PROFILE_DATAPATH = os.path.join(
        Const.CONFIGURATION_DIR,
        "mode-profile.face")
    VARYING_PARAMETERS = [
        'MEMORY_DIFF_ALGORITHM',
        'DISK_DIFF_ALGORITHM',
        'COMPRESSION_ALGORITHM_TYPE',
        'COMPRESSION_ALGORITHM_SPEED']

    LIVE_MIGRATION_FINISH_ASAP = 1
    LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE = 2
    LIVE_MIGRATION_STOP = LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE

    def __init__(self, num_cores=4):

        self.PROCESS_PIPELINED = True  # False: serialized processing

        # always have infinite queue for communicating with QEMU
        self.QUEUE_SIZE_MEMORY_SNAPSHOT = -1
        self.QUEUE_SIZE_MEMORY_DELTA_LIST = -1  # -1 for infinite
        self.QUEUE_SIZE_DISK_DELTA_LIST = -1  # -1 for infinite
        self.QUEUE_SIZE_OPTIMIZATION = -1  # one per DeltaImte
        self.QUEUE_SIZE_COMPRESSION = -1  # one per DeltaImte

        # number of CPU allocated
        VMOverlayCreationMode.set_num_cores(num_cores)

        self.OPTIMIZATION_DEDUP_BASE_DISK = True
        self.OPTIMIZATION_DEDUP_BASE_MEMORY = True
        self.OPTIMIZATION_DEDUP_BASE_SELF = True

        # "xdelta3", "bsdiff", "xor", "none"
        self.MEMORY_DIFF_ALGORITHM = "xdelta3"
        # "xdelta3", "bsdiff", "xor", "none"
        self.DISK_DIFF_ALGORITHM = "xdelta3"
        self.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_LZMA
        self.COMPRESSION_ALGORITHM_SPEED = 5  # 1 (fastest) ~ 9

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def update_mode(self, new_mode_dict):
        new_mode = dict()
        for key in self.VARYING_PARAMETERS:
            if new_mode_dict.get(key, None) is not None:
                new_mode[key] = new_mode_dict.get(key)
        self.__dict__.update(new_mode)

    @staticmethod
    def set_num_cores(num_cores):
        import psutil
        version = psutil.version_info
        # Installation of OpenStack Kilo uses pstuil version 1.2.1
        if version >= (2, 0):
            cpu_count = psutil.cpu_count()
            num_cores = min(cpu_count, num_cores)
            p = psutil.Process()
            desired_cpus = list(range(num_cores))
            p.cpu_affinity(desired_cpus)
            updated_cpu = p.cpu_affinity()
        else:
            # version below 2.0
            cpu_count = psutil.NUM_CPUS
            num_cores = min(cpu_count, num_cores)
            p = psutil.Process()
            desired_cpus = list(range(num_cores))
            p.set_cpu_affinity(desired_cpus)
            try:
                # psutil 1.2.1 has issue with large number of cores
                # https://github.com/giampaolo/psutil/issues/522
                updated_cpu = p.get_cpu_affinity()
            except OSError as e:
                updated_cpu = desired_cpus
                pass
        if desired_cpus != updated_cpu:
            raise Exception(
                "Cannot not set affinity mask: from %s to %s" %
                (desired_cpus, updated_cpu))

    @staticmethod
    def get_num_cores():
        import psutil
        version = psutil.version_info
        # Installation of OpenStack Kilo uses pstuil version 1.2.1
        p = psutil.Process()
        if version >= (2, 0):
            return len(p.cpu_affinity())
        else:
            try:
                # psutil 1.2.1 has issue with large number of cores
                # https://github.com/giampaolo/psutil/issues/522
                cores = len(p.get_cpu_affinity())
            except OSError as e:
                import affinity
                cores = len(affinity.sched_getaffinity(p.pid))
            return cores

    def get_mode_id(self):
        sorted_key = sorted(self.__dict__.keys())
        mode_str = list()
        for key in sorted_key:
            if key in self.VARYING_PARAMETERS:
                value = self.__dict__[key]
                mode_str.append("%s:%s" % (key, value))
        return "|".join(mode_str)

    @staticmethod
    def get_serial_single_process():
        VMOverlayCreationMode.MAX_THREAD_NUM = 1
        VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_ASAP
        mode = VMOverlayCreationMode(num_cores=1)
        num_cores = VMOverlayCreationMode.get_num_cores()
        if num_cores is not 1:
            raise CloudletGenerationError("Cannot allocate only 1 core")
        mode.PROCESS_PIPELINED = False
        return mode

    @staticmethod
    def get_pipelined_multi_process_finite_queue(num_cores=4):
        VMOverlayCreationMode.MAX_THREAD_NUM = 4
        mode = VMOverlayCreationMode(num_cores)
        update_cores = mode.get_num_cores()
        if update_cores != num_cores:
            raise CloudletGenerationError(
                "Cannot allocate %d core" %
                num_cores)

        mode.PROCESS_PIPELINED = True
        # always have infinite queue for communicating with QEMU
        mode.QUEUE_SIZE_MEMORY_SNAPSHOT = -1
                                                # Otherwise, QEMU will be
                                                # automatically throttled
        mode.QUEUE_SIZE_DISK_DELTA_LIST = 4
        mode.QUEUE_SIZE_MEMORY_DELTA_LIST = 4
        mode.QUEUE_SIZE_OPTIMIZATION = 4
        mode.QUEUE_SIZE_COMPRESSION = 4
        return mode


class Synthesis_Const(object):
    # PIPLINING CONSTANT
    TRANSFER_SIZE = 1024*16
    END_OF_FILE = "!!Overlay Transfer End Marker"
    ERROR_OCCURED = "!!Overlay Transfer Error Marker"

    # Synthesis Server
    LOCAL_IPADDRESS = 'localhost'
    SERVER_PORT_NUMBER = 8021

