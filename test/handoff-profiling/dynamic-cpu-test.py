#!/usr/bin/env python

import os
import sys
sys.path.insert(0, "../../")

import psutil
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
from elijah.provisioning.handoff import _handoff_start_time


class CPUCoreControl(threading.Thread):
    def __init__(self):
        self.stop = threading.Event()
        # [(time1, [cores]), (time2, [cores]), ...]
        self.cpu_bw_changes = [(100.0, [1, 2])]
        threading.Thread.__init__(self, target=self.core_change)

    def _set_affinity_chilren(self, core_list):
        proc_list = list()
        cur_proc = psutil.Process(os.getpid())
        proc_list.append(cur_proc)
        while True:
            if len(proc_list) == 0:
                break
            proc = proc_list.pop(0)
            if proc.name.startswith("python"):
                proc.set_cpu_affinity(core_list)
                proc_list += proc.get_children()
                LOG.info("control_core\t%s\t%s" % (proc.name, core_list))

    def core_change(self):
        global _handoff_start_time

        (activate_time, core_list) = self.cpu_bw_changes.pop(0)
        while(not self.stop.wait(1)):
            duration = time.time() - _handoff_start_time[0]
            LOG.info("control_core\t%f\t%f\t%s" % (activate_time, duration, core_list))
            if activate_time <= duration:
                # change number of core
                self._set_affinity_chilren(core_list)
                current_cores = VMOverlayCreationMode.get_num_cores()
                VMOverlayCreationMode.set_num_cores(len(core_list))
                LOG.info("control_core\t%f\tupdate all children to %s" % (duration, core_list))
                try:
                    (activate_time, core_list) = self.cpu_bw_changes.pop(0)
                except IndexError as e:
                    LOG.info("control_core\tno more data")
                    break
            else:
                continue
        LOG.info("control_core\tfinish bw control thread")

    def terminate(self):
        self.stop.set()

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
        (windows_base_path, mar),
        #(windows_base_path, face),
        #(linux_base_path, moped),
        #(linux_base_path, speech),
        #(linux_base_path, random),
        #(linux_base_path, fluid),
    ]
    for (base_path, overlay_path) in workloads:
        if os.path.exists(base_path) == False:
            raise ProfilingError("Invalid path to %s" % base_path)
        if os.path.exists(overlay_path) == False:
            raise ProfilingError("Invalid path to %s" % overlay_path)

    num_core = 1
    bandwidth = [15]*10
    for (base_path, overlay_path) in workloads:
        for network_bw in bandwidth:
            # confiure network using TC
            cmd = "sudo %s restart %d" % (os.path.abspath("./traffic_shaping"), network_bw)
            LOG.debug(cmd)
            LOG.debug(subprocess.check_output(cmd.split(" ")))
            VMOverlayCreationMode.USE_STATIC_NETWORK_BANDWIDTH = network_bw

            # start controlling number of cores
            cpu_control = CPUCoreControl()
            cpu_control.start()

            # generate mode
            VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE
            overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue(num_cores=num_core)
            overlay_mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_GZIP
            overlay_mode.COMPRESSION_ALGORITHM_SPEED = 1
            overlay_mode.MEMORY_DIFF_ALGORITHM = "none"
            overlay_mode.DISK_DIFF_ALGORITHM = "none"

            overlay_mode.set_num_cores(num_core)
            LOG.debug("network-test\t%s-varying (Mbps)" % (num_core))
            is_url, overlay_url = PackagingUtil.is_zip_contained(overlay_path)
            run_network(base_path, overlay_url, overlay_mode)

            cpu_control.terminate()
            cpu_control.join()
            time.sleep(30)

