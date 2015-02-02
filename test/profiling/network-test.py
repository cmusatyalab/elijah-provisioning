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

from profile_stage import *


def network_test(base_path, overlay_path, overlay_mode):
    try:
        synthesis.synthesis(base_path, overlay_path,
                            return_residue="network:blizzard.elijah.cs.cmu.edu",
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
        #(linux_base_path, speech),
        #(windows_base_path, mar),
        #(windows_base_path, face),
        #(linux_base_path, random),
        #(linux_base_path, fluid),
    ]
    for (base_path, overlay_path) in workloads:
        if os.path.exists(base_path) == False:
            raise ProfilingError("Invalid path to %s" % base_path)
        if os.path.exists(overlay_path) == False:
            raise ProfilingError("Invalid path to %s" % overlay_path)

    VMOverlayCreationMode.MAX_THREAD_NUM = 1
    #VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE
    bandwidth = [1, 5, 10, 20, 30, 40, 50, 50]
    #bandwidth = [20]
    bandwidth.reverse()
    for (base_path, overlay_path) in workloads:
        for network_bw in bandwidth:
            for stop_condition in [VMOverlayCreationMode.LIVE_MIGRATION_FINISH_ASAP]:
                # generate mode
                VMOverlayCreationMode.LIVE_MIGRATION_STOP = stop_condition
                overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue(num_cores=1)
                overlay_mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_BZIP2
                overlay_mode.COMPRESSION_ALGORITHM_SPEED = 5
                overlay_mode.MEMORY_DIFF_ALGORITHM = "xdelta3"
                overlay_mode.DISK_DIFF_ALGORITHM = "xdelta3"

                VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps = network_bw
                LOG.debug("network-test\t%s (Mbps)" % VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps)
                is_url, overlay_url = PackagingUtil.is_zip_contained(overlay_path)
                run_profile(base_path, overlay_url, overlay_mode)
                time.sleep(10)

