#!/usr/bin/env python

import os
import sys
sys.path.insert(0, "../../")

import subprocess
import traceback
from datetime import datetime
import json
import time
import threading

from elijah.provisioning.configuration import Const
Const.LOG_PATH = os.path.join(os.path.abspath(os.curdir), "log-%s" % str(datetime.now()))
import logging
LOG = logging.getLogger(__name__)

from elijah.provisioning.configuration import VMOverlayCreationMode
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning.package import PackagingUtil



def run_file(base_path, overlay_path, overlay_mode):
    try:
        synthesis.synthesis(base_path, overlay_path,
                            handoff_url="file://",
                            zip_container=True,
                            overlay_mode=overlay_mode,
                            is_profiling_test=True)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
        sys.stderr.write("%s\nFailed to synthesize" % str(traceback.format_exc()))


def run_network(base_path, overlay_path, overlay_mode):
    try:
        synthesis.synthesis(base_path, overlay_path,
                            handoff_url="tcp://rain.elijah.cs.cmu.edu",
                            zip_container=True,
                            overlay_mode=overlay_mode,
                            is_profiling_test=True)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
        sys.stderr.write("%s\nFailed to synthesize" % str(traceback.format_exc()))


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
    workloads = [
        (linux_base_path, moped),
        (linux_base_path, fluid),
        (windows_base_path, mar),
        (windows_base_path, face),
        #(linux_base_path, random),
        #(linux_base_path, speech),
    ]
    for (base_path, overlay_path) in workloads:
        if os.path.exists(base_path) == False:
            raise ProfilingError("Invalid path to %s" % base_path)
        if os.path.exists(overlay_path) == False:
            raise ProfilingError("Invalid path to %s" % overlay_path)

    num_core = 1
    bandwidth = [5, 10, 15, 20, 25, 30, 30]
    bandwidth.reverse()
    #num_cores_list = [4,4,3,2,1]; network_bw = 10

    for (base_path, overlay_path) in workloads:
        for network_bw in bandwidth:
        #for num_core in num_cores_list:
            # confiure network using TC
            cmd = "sudo %s restart %d" % (os.path.abspath("./traffic_shaping"), network_bw)
            LOG.debug(cmd)
            LOG.debug(subprocess.check_output(cmd.split(" ")))
            #VMOverlayCreationMode.USE_STATIC_NETWORK_BANDWIDTH = network_bw

            # generate mode
            VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE
            overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue(num_cores=num_core)
            overlay_mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_GZIP
            overlay_mode.COMPRESSION_ALGORITHM_SPEED = 1
            overlay_mode.MEMORY_DIFF_ALGORITHM = "none"
            overlay_mode.DISK_DIFF_ALGORITHM = "none"

            LOG.debug("network-test\t%s-%s (Mbps)" % (network_bw, num_core))
            is_url, overlay_url = PackagingUtil.is_zip_contained(overlay_path)
            #run_file(base_path, overlay_url, overlay_mode)
            run_network(base_path, overlay_url, overlay_mode)

            time.sleep(30)

