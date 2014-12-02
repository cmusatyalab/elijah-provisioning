#
# cloudlet-process manager
#
#   author: Kiryong Ha <krha@cmu.edu>
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
import multiprocessing
import threading
import time
import ctypes
import sys
import traceback
import Queue
from Configuration import Const
from Configuration import VMOverlayCreationMode

from migration_profile import MigrationMode
from migration_profile import ModeProfileError
from migration_profile import ModeProfile
import log as logging


LOG = logging.getLogger(__name__)
_process_controller = None



def get_instance():
    global _process_controller

    if _process_controller == None:
        _process_controller = ProcessManager()
        _process_controller.daemon = True
        _process_controller.start()
    return _process_controller



class ProcessManagerError(Exception):
    pass


class ProcessManager(threading.Thread):
    def __init__(self):
        self.overlay_creation_mode = None
        self.manager = multiprocessing.Manager()
        self.process_list = dict()
        self.process_infos = self.manager.dict()
        self.process_control = dict()
        self.stop = threading.Event()
        self.overlay_creation_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()
        self.migration_dest = "network"

        # load profiling information
        profile_path = os.path.abspath(VMOverlayCreationMode.PROFILE_DATAPATH)
        if os.path.exists(profile_path) == False:
            raise ProcessManagerError("Cannot load profile at : %s" % profile_path)
        self.mode_profile = ModeProfile.load_from_file(profile_path)
        super(ProcessManager, self).__init__(target=self.start_managing)

    def set_mode(self, new_mode, migration_dest):
        self.overlay_creation_mode = new_mode
        self.migration_dest = migration_dest

    def _send_query(self, query, worker_names, data=None):
        sent_worker_name = list()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            process_info = self.process_infos[worker_name]
            if process_info['is_alive'] == True:
                control_queue.put(query)
                if data is not None:
                    control_queue.put(data)
                sent_worker_name.append(worker_name)
            else:
                pass
                #sys.stdout.write("not sending query to %s since it's over\n" %\
                #                 worker_name)
        return sent_worker_name

    def _recv_response(self, query, worker_names):
        response_dict = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            response_dict[worker_name] = (None, 0)
            process_info = self.process_infos[worker_name]
            if process_info['is_alive'] == True:
                try:
                    if worker.is_alive():
                        response = response_queue.get(timeout=1)
                        response_dict[worker_name] = (response, 0)
                except Queue.Empty as e:
                    msg = "Error, Cannot receive response from: %s process\n"%\
                        (str(worker_name))
                    sys.stderr.write(msg)
        return response_dict

    def _change_comp_mode(self, comp_type, comp_level):
        worker_names = self.process_list.keys()
        if "CompressProc" in worker_names:
            self._send_query("change_mode",
                             ["CompressProc"],
                             data={
                                 "comp_type":comp_type,
                                 "comp_level":comp_level
                             }
                             )

    def _change_disk_diff_mode(self, diff_algorithm):
        worker_names = self.process_list.keys()
        if "CreateDiskDeltalist" in worker_names:
            self._send_query("change_mode",
                            ["CreateDiskDeltalist"],
                            data={"diff_algorithm":diff_algorithm})

    def _change_memory_diff_mode(self, diff_algorithm):
        worker_names = self.process_list.keys()
        if "CreateMemoryDeltalist" in worker_names:
            self._send_query("change_mode",
                            ["CreateMemoryDeltalist"],
                            data={"diff_algorithm":diff_algorithm})

    def _get_cpu_usage(self):
        result = dict()
        query = "cpu_usage_accum"   #"current_bw"
        worker_names = self.process_list.keys()
        worker_names = self._send_query(query, worker_names)

        responses = self._recv_response(query, worker_names)
        for worker_name, (response, duration) in responses.iteritems():
            #sys.stdout.write("[manager] %s:\t%s:\t%s\t(%f s)\n" % (query, worker_name, str(response), duration))
            result[worker_name] = response
        return result

    def _get_queue_length(self):
        worker_names = self.process_list.keys()
        responses = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            response = (worker.monitor_current_inqueue_length.value, worker.monitor_current_outqueue_length.value)
            responses[worker_name] = response
        return responses

    def _get_queueing_time(self):
        result = dict()
        worker_names = self.process_list.keys()
        responses = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            response = (worker.monitor_current_get_time.value, worker.monitor_current_put_time.value)
            responses[worker_name] = response

        sys.stdout.write("[manager]\t")
        for (worker_name, response) in responses.iteritems():
            sys.stdout.write("%s(%s)\t" % (worker_name[:10], str(response)))
        sys.stdout.write("\n")
        return result

    def get_system_speed(self):
        worker_names = ["DeltaDedup", "CreateMemoryDeltalist",
                        "CreateDiskDeltalist", "CompressProc"]
        p_dict = dict()
        r_dict = dict()
        p_dict_cur = dict()
        r_dict_cur = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            if worker == None:
                return None
            process_info = self.process_infos[worker_name]
            if process_info['finish_processing_input'] == True:
                #print "%s is finished" % worker_name
                return None
            time_block = worker.monitor_total_time_block.value
            ratio_block = worker.monitor_total_ratio_block.value
            time_block_cur = worker.monitor_total_time_block_cur.value
            ratio_block_cur = worker.monitor_total_ratio_block_cur.value
            if time_block <= 0 or ratio_block <=0:
                return None
            p_dict[worker_name] = time_block
            r_dict[worker_name] = ratio_block
            p_dict_cur[worker_name] = time_block_cur
            r_dict_cur[worker_name] = ratio_block_cur

        # Get P and R
        total_p = MigrationMode.get_total_P(p_dict)
        total_r = MigrationMode.get_total_R(r_dict)
        total_p_cur = MigrationMode.get_total_P(p_dict_cur)
        total_r_cur = MigrationMode.get_total_R(r_dict_cur)
        system_out_bw_mbps = MigrationMode.get_system_throughput(self.overlay_creation_mode.NUM_PROC_COMPRESSION,
                                                    total_p,
                                                    total_r)
        system_out_bw_mbps_cur = MigrationMode.get_system_throughput(self.overlay_creation_mode.NUM_PROC_COMPRESSION,
                                                    total_p_cur,
                                                    total_r_cur)
        #sys.stdout.write("P: %f, %f \tR:%f, %f, BW: %f, %f mbps\t(%f,%f,%f,%f), (%f,%f,%f,%f), (%f,%f,%f,%f), (%f,%f,%f,%f)\n" % \
        #                 (total_p, total_p_cur,
        #                  total_r, total_r_cur,
        #                  system_out_bw_mbps, system_out_bw_mbps_cur,
        #                  p_dict['CreateDiskDeltalist'],
        #                  p_dict['CreateMemoryDeltalist'],
        #                  p_dict['DeltaDedup'],
        #                  p_dict['CompressProc'],
        #                  r_dict['CreateDiskDeltalist'],
        #                  r_dict['CreateMemoryDeltalist'],
        #                  r_dict['DeltaDedup'],
        #                  r_dict['CompressProc'],
        #                  p_dict_cur['CreateDiskDeltalist'],
        #                  p_dict_cur['CreateMemoryDeltalist'],
        #                  p_dict_cur['DeltaDedup'],
        #                  p_dict_cur['CompressProc'],
        #                  r_dict_cur['CreateDiskDeltalist'],
        #                  r_dict_cur['CreateMemoryDeltalist'],
        #                  r_dict_cur['DeltaDedup'],
        #                  r_dict_cur['CompressProc']
        #                  ))

        return p_dict, r_dict, system_out_bw_mbps, p_dict_cur, r_dict_cur, system_out_bw_mbps_cur

    def get_network_speed(self):
        if self.migration_dest.startswith("network"):
            worker = self.process_list.get("StreamSynthesisClient", None)
            if worker == None:
                return None
            process_info = self.process_infos["StreamSynthesisClient"]
            if process_info['is_alive'] == False:
                return None
            network_bw_mbps = worker.monitor_network_bw.value
            if network_bw_mbps <= 0:
                return None
            return network_bw_mbps # mbps
        else:
            return 1024*1024*200*8 # disk speed (200 MBps)
            #return 8.672088

    def start_managing(self):
        time_s = time.time()
        mode_change_history = list()
        time_prev_mode_change = time_s
        count = 0
        self.cpu_statistics = list()
        while (not self.stop.wait(0.1)):
            try:
                network_bw_mbps = self.get_network_speed()  # mega bit/s
                system_speed = self.get_system_speed()
                time_current_iter = time.time()
                if system_speed == None:
                    #sys.stdout.write("system speed is not measured\n")
                    continue
                if network_bw_mbps == None:
                    #sys.stdout.write("network speed is not measured\n")
                    continue
                p_dict, r_dict, system_bw_mbps, p_duct_cur, r_dict_cur, system_bw_mbps_cur  = system_speed
                LOG.debug("throughput\t%f\tsystem(mbps):%f,%f\tnetwork((mbps):%f" % (time_current_iter,
                                                                                     system_bw_mbps,
                                                                                     system_bw_mbps_cur,
                                                                                     network_bw_mbps))

                # get new mode
                #if (time_current_iter-time_prev_mode_change) > 5:   # apply after 5 seconds
                if count == -1 and len(mode_change_history) == 0:
                    # use current throughput
                    new_mode = self.mode_profile.predict_new_mode(self.overlay_creation_mode,
                                                                p_dict, r_dict,
                                                                system_bw_mbps_cur,
                                                                network_bw_mbps)
                    diff_mode = MigrationMode.mode_diff(self.overlay_creation_mode.__dict__, new_mode.__dict__)
                    if diff_mode is not None and len(diff_mode) > 0:
                        print "%s\n%s" % (new_mode, diff_mode)
                        # check compression
                        new_comp_level = None
                        new_comp_type = None
                        new_disk_diff = None
                        new_memory_diff = None
                        if "COMPRESSION_ALGORITHM_SPEED" in diff_mode.keys():
                            new_comp_level = diff_mode["COMPRESSION_ALGORITHM_SPEED"]
                        if "COMPRESSION_ALGORITHM_TYPE" in diff_mode.keys():
                            new_comp_type = diff_mode["COMPRESSION_ALGORITHM_TYPE"]
                        if "DISK_DIFF_ALGORITHM" in diff_mode.keys():
                            new_disk_diff = diff_mode["DISK_DIFF_ALGORITHM"]
                        if "MEMORY_DIFF_ALGORITHM" in diff_mode.keys():
                            new_memory_diff = diff_mode["MEMORY_DIFF_ALGORITHM"]

                        # apply change
                        if new_comp_type is not None or new_comp_level is not None:
                            self._change_comp_mode(new_comp_type, new_comp_level)
                        if new_disk_diff is not None:
                            self._change_disk_diff_mode(new_disk_diff)
                        if new_memory_diff is not None:
                            self._change_memory_diff_mode(new_memory_diff)

                        mode_change_history.append((time_current_iter, self.overlay_creation_mode, new_mode))
                        time_prev_mode_change = time_current_iter
                        self.overlay_creation_mode = new_mode
                '''
                '''

                pass
                #result = self._get_cpu_usage()
                #self.cpu_statistics.append((time.time()-time_s, result))
                #time.sleep(1)
                #self._change_comp_mode()
                #break
                #result = self._get_queue_length()
                #time.sleep(0.1)
            except Exception as e:
                sys.stdout.write("[manager] Exception")
                sys.stderr.write(traceback.format_exc())
                sys.stderr.write("%s\n" % str(e))
            count += 1

    def register(self, worker):
        worker_name = getattr(worker, "worker_name", "NoName")
        process_info = self.manager.dict()
        process_info['update_period'] = 0.1 # seconds
        process_info['is_alive'] = True
        process_info['finish_processing_input'] = False
        control_queue = multiprocessing.Queue()
        response_queue = multiprocessing.Queue()
        #print "[manager] register new process: %s" % worker_name

        self.process_list[worker_name] = worker
        self.process_infos[worker_name] = (process_info)
        self.process_control[worker_name] = (control_queue, response_queue)
        return process_info, control_queue, response_queue

    def terminate(self):
        self.stop.set()


class ProcWorker(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        self.worker_name = str(kwargs.pop('worker_name', self.__class__.__name__))
        process_manager = get_instance()
        (self.process_info, self.control_queue, self.response_queue) = \
            process_manager.register(self)  # shared dictionary

        # measurement
        self.monitor_total_time_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.monitor_total_ratio_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.monitor_total_time_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)
        self.monitor_total_ratio_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)
        self.in_size = 0
        self.out_size = 0

        # not used
        self.monitor_current_bw = float(0)
        self.monitor_current_inqueue_length = multiprocessing.Value('d', -1.0)
        self.monitor_current_outqueue_length = multiprocessing.Value('d', -1.0)
        self.monitor_current_get_time = multiprocessing.Value('d', -1.0)
        self.monitor_current_put_time = multiprocessing.Value('d', -1.0)
        super(ProcWorker, self).__init__(*args, **kwargs)

    def _handle_control_msg(self, control_msg):
        if control_msg == "current_bw":
            self.response_queue.put(self.monitor_current_bw)
            return True
        elif control_msg == "queue_length":
            return (self.monitor_current_inqueue_length, self.monitor_current_outqueue_length)
        elif control_msg == "cpu_usage_accum":
            #(utime, stime, child_utime, child_stime, elaspe_time) = os.times()
            #all_times = utime+stime+child_utime+child_stime
            self.response_queue.put(os.times())
            return True
        else:
            #sys.stdout.write("Cannot be handled in super class\n")
            return False



class TestProc(ProcWorker):
    def __init__(self, worker_name):
        super(TestProc, self).__init__(target=self.read_mem_snapshot)

    def read_mem_snapshot(self):
        print "launch process: %s" % (self.worker_name)


if __name__ == "__main__":
    test = TestProc("test")
    test.start()
    test.join()
    print "end"

