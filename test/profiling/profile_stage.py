#!/usr/bin/env python

import os
import sys
sys.path.insert(0, "../../")
import traceback
from datetime import datetime
import json
import time

from elijah.provisioning.Configuration import Const
Const.LOG_PATH = os.path.join(os.path.abspath(os.curdir), "log-%s" % str(datetime.now()))
from elijah.provisioning import log as logging
LOG = logging.getLogger(__name__)

from elijah.provisioning.Configuration import VMOverlayCreationMode
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning.package import PackagingUtil

try:
    import affinity
except ImportError as e:
    sys.stderr.write("Cannot find affinity package\n")
    sys.exit(1)




class ProfilingError(Exception):
    pass


def run_profile(base_path, overlay_path, overlay_mode):
    try:
        synthesis.synthesis(base_path, overlay_path,
                            return_residue="file",
                            zip_container=True,
                            overlay_mode=overlay_mode,
                            is_profiling_test=True)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
        sys.stderr.write("%s\nFailed to synthesize" % str(traceback.format_exc()))


def profiling_workload():
    NUM_CPU_CORES = 1
    mode_list = list()
    for repeat in xrange(3):
        for diff in ("xor", "xdelta3"):    #("xdelta3", "bsdiff", "none"):
            for comp_type in (Const.COMPRESSION_LZMA, Const.COMPRESSION_BZIP2, Const.COMPRESSION_GZIP):
                for comp_level in [1, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
                    overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue(num_cores=NUM_CPU_CORES)
                    overlay_mode.COMPRESSION_ALGORITHM_TYPE = comp_type
                    overlay_mode.COMPRESSION_ALGORITHM_SPEED = comp_level
                    overlay_mode.MEMORY_DIFF_ALGORITHM = diff
                    overlay_mode.DISK_DIFF_ALGORITHM = diff
                    mode_list.append(overlay_mode)
    return mode_list


def validation_mode():
    mode_list = list()
    core = 4

    mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()
    mode.NUM_PROC_DISK_DIFF = core
    mode.NUM_PROC_MEMORY_DIFF = core
    mode.NUM_PROC_OPTIMIZATION = core
    mode.NUM_PROC_COMPRESSION = core
    mode_list.append(mode)

    return mode_list

def serial_vs_pipe():
    # serial vs pipeline
    mode = VMOverlayCreationMode.get_serial_single_process()
    mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_LZMA
    mode.COMPRESSION_ALGORITHM_SPEED = 5
    mode.MEMORY_DIFF_ALGORITHM = "xdelta3"
    mode.DISK_DIFF_ALGORITHM = "xdelta3"
    mode.NUM_PROC_DISK_DIFF = core
    mode.NUM_PROC_MEMORY_DIFF = core
    mode.NUM_PROC_OPTIMIZATION = core
    mode.NUM_PROC_COMPRESSION = core
    mode_list.append(mode)
    mode_list = list()

    return mode_list

def scaling_test():
    mode_list = list()
    # scale
    for core in (1,1,4): #(1, 1,2,3,4):
        mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()
        mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_LZMA
        mode.COMPRESSION_ALGORITHM_SPEED = 5
        mode.MEMORY_DIFF_ALGORITHM = "xdelta3"
        mode.DISK_DIFF_ALGORITHM = "xdelta3"
        mode.NUM_PROC_DISK_DIFF = core
        mode.NUM_PROC_MEMORY_DIFF = core
        mode.NUM_PROC_OPTIMIZATION = core
        mode.NUM_PROC_COMPRESSION = core
        mode_list.append(mode)

    return mode_list


def performance_of_each_stage():
    mode_list = list()
    core = 1
    '''
    # performance of each stage
    mode = VMOverlayCreationMode.get_pipelined_single_process()
    mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_LZMA
    mode.COMPRESSION_ALGORITHM_SPEED = 5
    mode.MEMORY_DIFF_ALGORITHM = "xdelta3"
    mode.DISK_DIFF_ALGORITHM = "xdelta3"
    mode.NUM_PROC_DISK_DIFF = core
    mode.NUM_PROC_MEMORY_DIFF = core
    mode.NUM_PROC_OPTIMIZATION = core
    mode.NUM_PROC_COMPRESSION = core
    mode_list.append(mode)
    '''

    return mode_list

if __name__ == "__main__":
    # set input workloads
    linux_base_path = "/home/krha/cloudlet/image/portable/precise.raw"
    windows_base_path = "/home/krha/cloudlet/image/window7-enterprise-x86/window7.raw"
    face = "/home/krha/cloudlet/image/overlay/vmhandoff/face-overlay.zip"
    mar = "/home/krha/cloudlet/image/overlay/vmhandoff/mar-overlay.zip"
    moped = "/home/krha/cloudlet/image/overlay/vmhandoff/moped-overlay.zip"
    speech = "/home/krha/cloudlet/image/overlay/vmhandoff/speech-overlay.zip"
    fluid = "/home/krha/cloudlet/image/overlay/vmhandoff/fluid-overlay.zip"
    random = "/home/krha/cloudlet/image/overlay/vmhandoff/overlay-random-100mb.zip"
    delta = "/home/krha/cloudlet/image/overlay/vmhandoff/workload-delta.zip"
    workloads = [
        (linux_base_path, delta),
        #(linux_base_path, moped),
        #(linux_base_path, random),
        #(windows_base_path, mar),
        #(linux_base_path, speech),
        #(windows_base_path, face),
        #(linux_base_path, fluid),
    ]
    for (base_path, overlay_path) in workloads:
        if os.path.exists(base_path) == False:
            raise ProfilingError("Invalid path to %s" % base_path)
        if os.path.exists(overlay_path) == False:
            raise ProfilingError("Invalid path to %s" % overlay_path)

    # generate mode
    mode_list = profiling_workload()
    #mode_list = validation_mode()

    # micro benchmarking
    #mode_list = scaling_test()
    #mode_list = serial_vs_pipe()

    VMOverlayCreationMode.MAX_THREAD_NUM = 1
    VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_ASAP
    #VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE
    for (base_path, overlay_path) in workloads:
        for each_mode in mode_list:
            is_url, overlay_url = PackagingUtil.is_zip_contained(overlay_path)
            run_profile(base_path, overlay_url, each_mode)
            time.sleep(10)

