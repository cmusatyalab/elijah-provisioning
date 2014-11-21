#!/usr/bin/env python

import os
import sys
sys.path.insert(0, "../../")
import traceback
from datetime import datetime
from elijah.provisioning.Configuration import Const
Const.LOG_PATH = "/home/krha/cloudlet/provisioning/log/profile-%s" % str(datetime.now())
from elijah.provisioning import log as logging
LOG = logging.getLogger(__name__)

from elijah.provisioning.Configuration import VMOverlayCreationMode
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning.package import PackagingUtil




class ProfilingError(Exception):
    pass


def run_profile(base_path, overlay_path, overlay_mode):
    LOG.debug("--------------------- new test ---------------------")
    LOG.debug("overlay: %s" % overlay_path)
    print overlay_mode

    try:
        synthesis.synthesis(base_path, overlay_path,
                            return_residue=True,
                            zip_container=True,
                            overlay_mode=overlay_mode,
                            is_profiling_test=True)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
        sys.stderr.write("%s\nFailed to synthesize" % str(traceback.format_exc()))


def generate_mode():
    mode_list = list()
    for memory_diff in ("xdelta3", "none"):
        for comp_type in (Const.COMPRESSION_LZMA, Const.COMPRESSION_BZIP2): 
            for comp_level in (1, 5, 9):
                overlay_mode = VMOverlayCreationMode.get_pipelined_single_process()
                overlay_mode.COMPRESSION_ALGORITHM_TYPE = comp_type
                overlay_mode.COMPRESSION_ALGORITHM_SPEED = comp_level
                mode_list.append(overlay_mode)
    return mode_list


if __name__ == "__main__":
    linux_base_path = "/home/krha/cloudlet/image/portable/precise.raw"
    windows_base_path = "/home/krha/cloudlet/image/window7-enterprise-x86/window7.raw"

    fluid = "/home/krha/cloudlet/image/overlay/vmhandoff/fluid-overlay.zip"
    moped = "/home/krha/cloudlet/image/overlay/vmhandoff/moped-overlay.zip"
    face = "/home/krha/cloudlet/image/overlay/vmhandoff/mar-overlay.zip"
    mar = "/home/krha/cloudlet/image/overlay/vmhandoff/face-overlay.zip"
    workloads = [
        (linux_base_path, moped),
        (linux_base_path, fluid),
        (windows_base_path, face),
        (windows_base_path, mar)
    ]

    for (base_path, overlay_path) in workloads:
        if os.path.exists(base_path) == False:
            raise ProfilingError("Invalid path to %s" % base_path)
        if os.path.exists(overlay_path) == False:
            raise ProfilingError("Invalid path to %s" % overlay_path)



    base_path = linux_base_path
    overlay_path = fluid
    is_url, overlay_path = PackagingUtil.is_zip_contained(overlay_path)
    mode_list = generate_mode()
    for each_mode in mode_list:
        run_profile(base_path, overlay_path, each_mode)
