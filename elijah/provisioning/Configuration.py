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
try:
    import affinity
except ImportError as e:
    sys.stderr.write("Cannot find affinity package\n")
    sys.exit(1)


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
    VERSION = str("0.9.1")
    HOME_DIR = os.path.abspath(os.path.expanduser("~"))
    CONFIGURATION_DIR = os.path.join('/', 'var', 'lib', 'cloudlet', 'conf')
    QUEUE_SUCCESS_MESSAGE       = "!!@#^&!MemorySnapshot Transfer SUCCESS Marker!!@#^&!"
    QUEUE_FAILED_MESSAGE        = "!!@#^&!MemorySnapshot Transfer FAILED Marker!!@#^&!"
    QUEUE_SUCCESS_MESSAGE_LEN   = len(QUEUE_SUCCESS_MESSAGE)
    QUEUE_FAILED_MESSAGE_LEN    = len(QUEUE_FAILED_MESSAGE)

    BASE_DISK               = ".base-img"
    BASE_MEM                = ".base-mem"
    BASE_DISK_META          = ".base-img-meta"
    BASE_MEM_META           = ".base-mem-meta"
    BASE_HASH_VALUE         = ".base-hash"
    OVERLAY_URIs            = ".overlay-URIs"
    OVERLAY_META            = "overlay-meta"
    OVERLAY_FILE_PREFIX     = "overlay-blob"
    OVERLAY_ZIP             = "overlay.zip"
    OVERLAY_LOG             = ".overlay-log"
    LOG_PATH                = "/var/tmp/cloudlet/log-synthesis"
    OVERLAY_BLOB_SIZE_KB    = 1024*1024 # 1G

    COMPRESSION_LZMA        = 1
    COMPRESSION_BZIP2       = 2
    COMPRESSION_GZIP        = 3

    META_BASE_VM_SHA256                 = "base_vm_sha256"
    META_RESUME_VM_DISK_SIZE            = "resumed_vm_disk_size"
    META_RESUME_VM_MEMORY_SIZE          = "resumed_vm_memory_size"
    META_OVERLAY_FILES                  = "overlay_files"
    META_OVERLAY_FILE_NAME              = "overlay_name"
    META_OVERLAY_FILE_COMPRESSION       = "overlay_compression"
    META_OVERLAY_FILE_SIZE              = "overlay_size"
    META_OVERLAY_FILE_DISK_CHUNKS       = "disk_chunk"
    META_OVERLAY_FILE_MEMORY_CHUNKS     = "memory_chunk"

    MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
    QEMU_BIN_PATH               = which("cloudlet_qemu-system-x86_64")
    FREE_MEMORY_BIN_PATH        = which("cloudlet_free_page_scan")
    CLOUDLETFS_PATH             = which("cloudlet_vmnetfs")
    XRAY_BIN_PATH               = which("cloudlet_disk_analyzer")

    # personal information
    CLOUDLET_DB             = os.path.abspath(os.path.join(HOME_DIR, ".cloudlet/config/cloudlet.db"))
    BASE_VM_DIR             = os.path.abspath(os.path.join(HOME_DIR, ".cloudlet", "baseVM"))

    # global configuration files
    CLOUDLET_DB_SCHEMA      = os.path.join(CONFIGURATION_DIR, "schema.sql")
    BASEVM_PACKAGE_SCHEMA   = os.path.join(CONFIGURATION_DIR, "package.xsd")
    TEMPLATE_XML            = os.path.join(CONFIGURATION_DIR, "VM_TEMPLATE.xml")
    TEMPLATE_OVF            = os.path.join(CONFIGURATION_DIR, "ovftransport.iso")
    CHUNK_SIZE=4096
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

        #check sanity
        if check_exist==True:
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
    TRIM_SUPPORT                        = True
    FREE_SUPPORT                        = False
    XRAY_SUPPORT                        = False
    DISK_ONLY                           = False
    ZIP_CONTAINER                       = False
    DATA_SOURCE_URI                     = None

    # see the effect of dedup and reducing semantic by generating two indenpendent overlay
    SEPERATE_DEDUP_REDUCING_SEMANTICS   = False
    # for test purposes, we can optionally save modified memory snapshot
    MEMORY_SAVE_PATH                    = None

    def __str__(self):
        import pprint
        return pprint.pformat(self.__dict__)


class VMOverlayCreationMode(object):
    PIPE_ONE_ELEMENT_SIZE = 4096*100 # 400KB == Max Pipe size is 1MB
    EMULATED_BANDWIDTH_Mbps = 100000 # Mbps
    MEASURE_AVERAGE_TIME    = 1 # seconds
    MAX_CPU_CORE = 4

    PROFILE_DATAPATH = os.path.abspath(os.path.join(Const.HOME_DIR, ".cloudlet/config/mode-profile"))

    LIVE_MIGRATION_FINISH_ASAP = 1
    LIVE_MIGRATION_FINISH_USE_SMAPSHOT_SIZE = 2
    LIVE_MIGRATION_STOP = LIVE_MIGRATION_FINISH_USE_SMAPSHOT_SIZE

    def __init__(self, num_cores=4):

        self.PROCESS_PIPELINED                      = True # False: serialized processing

        self.QUEUE_SIZE_MEMORY_SNAPSHOT             = -1 # always have infinite queue for communicating with QEMU
        self.QUEUE_SIZE_MEMORY_DELTA_LIST           = -1 # -1 for infinite
        self.QUEUE_SIZE_DISK_DELTA_LIST             = -1 # -1 for infinite
        self.QUEUE_SIZE_OPTIMIZATION                = -1 # one per DeltaImte
        self.QUEUE_SIZE_COMPRESSION                 = -1 # one per DeltaImte

        # number of CPU allocated
        self.set_num_cores(num_cores)

        self.MEMORY_DIFF_ALGORITHM                  = "xdelta3" # "xdelta3", "bsdiff", "none"
        self.DISK_DIFF_ALGORITHM                    = "xdelta3" # "xdelta3", "bsdiff", "none"

        self.OPTIMIZATION_DEDUP_BASE_DISK            = True
        self.OPTIMIZATION_DEDUP_BASE_MEMORY          = True
        self.OPTIMIZATION_DEDUP_BASE_SELF            = True

        self.COMPRESSION_ALGORITHM_TYPE              = Const.COMPRESSION_LZMA
        self.COMPRESSION_ALGORITHM_SPEED             = 5 # 1 (fastest) ~ 9

    def __str__(self):
        import pprint
        return pprint.pformat(self.__dict__)

    def update_mode(self, new_mode_dict):
        invalid_keys = ["NUM_PROC_DISK_DIFF", "NUM_PROC_MEMORY_DIFF", "NUM_PROC_COMPRESSION", "NUM_PROC_OPTIMIZATION"]
        for key in invalid_keys:
            if new_mode_dict.get(key, None) is not None:
                del new_mode_dict[key]
        self.__dict__.update(new_mode_dict)

    def set_num_cores(self, num_cores):
        # assuming 8 cores
        affinity_mask = 0x01
        if num_cores == 1:
            affinity_mask = 0x02     # cpu 1
        elif num_cores ==2:
            affinity_mask = 0x06    # cpu 1,2
        elif num_cores ==3:
            affinity_mask = 0x0e    # cpu 1,2,3
        elif num_cores ==4:
            affinity_mask = 0x1e # cpu 1,2,3,4
        else:
            raise IOException("Do not allocate more than 4 cores at this experiement")

        affinity.set_process_affinity_mask(os.getpid(), affinity_mask)
        updated_mask = affinity.get_process_affinity_mask(os.getpid())
        if affinity_mask != updated_mask:
            raise Exception("Cannot not set affinity mask: from %s to %s" % (affinity_mask, updated_mask))

        print "change num core to %d, affinity: %s, %s" % (num_cores, affinity_mask, updated_mask)
        self.NUM_PROC_MEMORY_DIFF = num_cores
        self.NUM_PROC_DISK_DIFF = num_cores
        self.NUM_PROC_OPTIMIZATION = num_cores
        self.NUM_PROC_COMPRESSION = num_cores

    def get_num_cores(self):
        num_cores = 0
        affinity_mask = affinity.get_process_affinity_mask(os.getpid())
        if affinity_mask == 0x02:     # cpu 1
            num_cores = 1
        elif affinity_mask == 0x06:    # cpu 1,2
            num_cores = 2
        elif affinity_mask == 0x0e:    # cpu 1,2,3
            num_cores = 3
        elif affinity_mask == 0x1e: # cpu 1,2,3,4
            num_cores = 4
        else:
            raise Exception("Do not allocate more than 4 cores at this experiement")
        return num_cores


    def get_mode_id(self):
        sorted_key = self.__dict__.keys()
        sorted_key.sort()
        mode_str = list()
        for key in sorted_key:
            if key in ["NUM_PROC_DISK_DIFF", "NUM_PROC_MEMORY_DIFF", "NUM_PROC_COMPRESSION", "NUM_PROC_OPTIMIZATION"]:
                continue
            value = self.__dict__[key]
            mode_str.append("%s:%s" % (key, value))
        return "|".join(mode_str)

    @staticmethod
    def from_dictionary(mode_dict):
        mode = VMOverlayCreationMode()
        diff = set(mode.__dict__.keys()) - set(mode_dict.keys())
        if diff:
            print "error, input dictionary is different: %s" % diff
            return None
        mode.__dict__.update(mode_dict)
        return mode


    @staticmethod
    def get_default():
        return VMOverlayCreationMode()

    @staticmethod
    def get_serial_single_process():
        mode = VMOverlayCreationMode(num_cores=1)
        num_cores = mode.get_num_cores()
        if num_cores is not 1:
            raise CloudletGenerationError("Cannot allocate only 1 core")
        mode.PROCESS_PIPELINED = False
        return mode

    @staticmethod
    def get_serial_multi_process(num_cores=4):
        mode = VMOverlayCreationMode(num_cores)
        update_cores = mode.get_num_cores()
        if update_cores != num_cores:
            raise CloudletGenerationError("Cannot allocate %d core" % num_cores)

        mode.PROCESS_PIPELINED = False
        return mode

    @staticmethod
    def get_pipelined_multi_process_finite_queue(num_cores=4):
        mode = VMOverlayCreationMode(num_cores)
        update_cores = mode.get_num_cores()
        if update_cores != num_cores:
            raise CloudletGenerationError("Cannot allocate %d core" % num_cores)

        mode.PROCESS_PIPELINED = True
        mode.QUEUE_SIZE_MEMORY_SNAPSHOT = -1    # always have infinite queue for communicating with QEMU
                                                # Otherwise, QEMU will be automatically throttled
        mode.QUEUE_SIZE_DISK_DELTA_LIST = 4
        mode.QUEUE_SIZE_MEMORY_DELTA_LIST = 4
        mode.QUEUE_SIZE_OPTIMIZATION = 4
        mode.QUEUE_SIZE_COMPRESSION = 4
        return mode



class Synthesis_Const(object):
    # PIPLINING CONSTANT
    TRANSFER_SIZE           = 1024*16
    END_OF_FILE             = "!!Overlay Transfer End Marker"
    ERROR_OCCURED           = "!!Overlay Transfer Error Marker"

    # Synthesis Server
    LOCAL_IPADDRESS = 'localhost'
    SERVER_PORT_NUMBER = 8021


class Caching_Const(object):
    MODULE_DIR          = os.path.dirname(os.path.abspath(__file__))
    CACHE_FUSE_BINPATH  = os.path.abspath(os.path.join(MODULE_DIR, "./caching/fuse/cachefs"))
    HOST_SAMBA_DIR      = "/var/samba/"
    CACHE_ROOT          = '/tmp/cloudlet_cache/'
    REDIS_ADDR          = ('localhost', 6379)
    REDIS_REQ_CHANNEL   = "fuse_request"
    REDIS_RES_CHANNEL   = "fuse_response"


