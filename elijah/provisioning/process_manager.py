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
import zmq

from .configuration import VMOverlayCreationMode
from .migration_profile import MigrationMode
from .migration_profile import ModeProfile
from . import log as logging


LOG = logging.getLogger(__name__)
_process_controller = None


def get_instance():
    global _process_controller

    if _process_controller is None:
        _process_controller = ProcessManager()
        _process_controller.daemon = True
        _process_controller.start()
    return _process_controller


def kill_instance():
    global _process_controller

    if _process_controller is not None:
        _process_controller.terminate()
        _process_controller = None


class ProcessManagerError(Exception):
    pass


class ProcessManager(threading.Thread):

    def __init__(self):
        self.overlay_creation_mode = None
        self.process_list = dict()
        self.process_infos = dict()
        self.process_control = dict()
        self.stop = threading.Event()
        self.migration_dest = "tcp"

        # load profiling information
        profile_path = os.path.abspath(VMOverlayCreationMode.PROFILE_DATAPATH)
        LOG.info("Load adaptation profile at: %s" % profile_path)
        if os.path.exists(profile_path) is False:
            raise ProcessManagerError(
                "Cannot load profile at : %s" % profile_path)
        self.mode_profile = ModeProfile.load_from_file(profile_path)

        if VMOverlayCreationMode.USE_PUBSUB_NETWORK_BANDWIDTH:
            self.mqctx = zmq.Context.instance()
            self.subscriber = self.mqctx.socket(zmq.SUB)
            self.subscriber.connect("tcp://localhost:5556")
            self.subscriber.setsockopt(zmq.SUBSCRIBE, b"bandwidth")
            self.new_bandwidth_published = False


        super(ProcessManager, self).__init__(target=self.start_managing)

    def set_mode(self, new_mode, migration_dest):
        self.overlay_creation_mode = new_mode
        self.migration_dest = migration_dest

    def _send_query(self, query, worker_names, data=None):
        sent_worker_name = list()
        for worker_name in worker_names:
            control_queue, response_queue = self.process_control[worker_name]
            worker_info = self.process_infos[worker_name]
            if worker_info['is_processing_alive'].value:
                control_queue.put(query)
                if data is not None:
                    control_queue.put(data)
                sent_worker_name.append(worker_name)
            else:
                pass
        return sent_worker_name

    def _recv_response(self, query, worker_names):
        response_dict = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            response_dict[worker_name] = (None, 0)
            worker_info = self.process_infos[worker_name]
            if worker_info['is_processing_alive'].value:
                try:
                    if worker.is_alive():
                        response = response_queue.get(timeout=1)
                        response_dict[worker_name] = (response, 0)
                except Queue.Empty as e:
                    msg = "Error, Cannot receive response from: %s process\n" %\
                        (str(worker_name))
                    sys.stderr.write(msg)
        return response_dict

    def _change_num_cores(self, new_num_cores):
        worker_names = self.process_list.keys()
        for worker_name in ["CreateMemoryDeltalist", "CreateDiskDeltalist",
                            "CompressProc", "DeltaDedup"]:
            if worker_name in worker_names:
                self._send_query("change_cores",
                                 [worker_name],
                                 data={"num_cores": new_num_cores})

    def _change_comp_mode(self, comp_type, comp_level):
        worker_names = self.process_list.keys()
        if "CompressProc" in worker_names:
            self._send_query("change_mode", ["CompressProc"],
                             data={"comp_type": comp_type,
                                   "comp_level": comp_level
                                   }
                             )

    def _change_disk_diff_mode(self, diff_algorithm):
        worker_names = self.process_list.keys()
        if "CreateDiskDeltalist" in worker_names:
            self._send_query("change_mode", ["CreateDiskDeltalist"],
                             data={"diff_algorithm": diff_algorithm})

    def _change_memory_diff_mode(self, diff_algorithm):
        worker_names = self.process_list.keys()
        if "CreateMemoryDeltalist" in worker_names:
            self._send_query("change_mode", ["CreateMemoryDeltalist"],
                             data={"diff_algorithm": diff_algorithm})

    def _get_queue_length(self):
        worker_names = self.process_list.keys()
        responses = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            response = (
                worker.monitor_current_inqueue_length.value,
                worker.monitor_current_outqueue_length.value)
            responses[worker_name] = response
        return responses

    def _get_queueing_time(self):
        result = dict()
        worker_names = self.process_list.keys()
        responses = dict()
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            response = (worker.monitor_current_get_time.value,
                        worker.monitor_current_put_time.value)
            responses[worker_name] = response

        sys.stdout.write("[manager]\t")
        for (worker_name, response) in responses.iteritems():
            sys.stdout.write("%s(%s)\t" % (worker_name[:10], str(response)))
        sys.stdout.write("\n")
        return result

    @staticmethod
    def _averaged_value(measure_hist,
                        cur_time,
                        avg_time=VMOverlayCreationMode.MEASURE_AVERAGE_TIME):
        avg_value = float(0)
        counter = 0
        for (measured_time, value) in reversed(measure_hist):
            if cur_time - measured_time > avg_time:
                break
            avg_value += value
            counter += 1
        return float(avg_value)/counter

    def get_system_speed(self):
        worker_names = ["DeltaDedup", "CreateMemoryDeltalist",
                        "CreateDiskDeltalist", "CompressProc"]
        total_size_dict_in = dict()
        total_size_dict_out = dict()
        cur_size_dict_in = dict()
        cur_size_dict_out = dict()
        p_dict = dict()
        r_dict = dict()
        p_dict_cur = dict()
        r_dict_cur = dict()

        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            if worker is None:
                #LOG.debug("%f\t%s is not available" % (time.time(), worker_name))
                return None
            worker_info = self.process_infos[worker_name]
            if worker_info['finish_processing_input'].value:
                #LOG.debug("%f\t%s is finished" % (time.time(), worker_name))
                return None
            time_block = worker.monitor_total_time_block.value
            ratio_block = worker.monitor_total_ratio_block.value
            time_block_cur = worker.monitor_total_time_block_cur.value
            ratio_block_cur = worker.monitor_total_ratio_block_cur.value
            if time_block <= 0 or ratio_block <= 0:
                #LOG.debug("%s has wrong data" % worker_name)
                return None
            p_dict[worker_name] = time_block
            r_dict[worker_name] = ratio_block
            p_dict_cur[worker_name] = time_block_cur
            r_dict_cur[worker_name] = ratio_block_cur
            total_size_dict_in[worker_name] = worker.monitor_total_input_size.value
            total_size_dict_out[worker_name] = worker.monitor_total_output_size.value
            cur_size_dict_in[worker_name] = worker.monitor_total_input_size_cur.value
            cur_size_dict_out[worker_name] = worker.monitor_total_output_size_cur.value

        # Get total average P and total R
        memory_in_size = (total_size_dict_in['CreateMemoryDeltalist'])
        disk_in_size = (total_size_dict_in['CreateDiskDeltalist'])
        alpha = float(memory_in_size)/(memory_in_size+disk_in_size)
        total_p = MigrationMode.get_total_P(p_dict, alpha)
        total_r = MigrationMode.get_total_R(r_dict, alpha)

        # Get total instant P and total R
        alpha_cur = float(memory_in_size)/(memory_in_size+disk_in_size)
        total_p_cur = MigrationMode.get_total_P(p_dict_cur, alpha_cur)
        total_r_cur = MigrationMode.get_total_R(r_dict_cur, alpha_cur)
        num_cores = VMOverlayCreationMode.get_num_cores()
        system_block_per_sec, system_in_bw_est, system_out_bw_est =\
            MigrationMode.get_system_throughput(num_cores, total_p, total_r)
        system_block_per_sec_cur, system_in_bw_cur_est, system_out_bw_cur_est =\
            MigrationMode.get_system_throughput(num_cores, total_p_cur, total_r_cur)
        #LOG.debug("p tot\t%s\t%f --> %f" % (str(p_dict), alpha, total_p))
        #LOG.debug("p cur\t%s\t%f --> %f" % (str(p_dict_cur), alpha, total_p_cur))
        # sys.stdout.write("P: %f, %f \tR:%f, %f, BW: %f, %f mbps\t(%f,%f,%f,%f), (%f,%f,%f,%f), (%f,%f,%f,%f), (%f,%f,%f,%f)\n" % \
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

        # get actual system throughput using in out size
        system_output_size = total_size_dict_out['CompressProc']
        system_in_size = total_size_dict_in[
            'CreateDiskDeltalist'] + total_size_dict_in['CreateMemoryDeltalist']
        cur_time = time.time()
        duration = cur_time - self.prev_measured_time
        system_in_bw_actual = self.prev_system_in_bw
        system_out_bw_actual = self.prev_system_out_bw
        if (system_in_size > self.prev_system_in_size) and\
                (system_output_size > self.prev_system_out_size):
            system_in_bw_instant = 8.0 * \
                (system_in_size-self.prev_system_in_size)/duration/1024/1024
            system_out_bw_instant = 8.0 * \
                (system_output_size-self.prev_system_out_size)/duration/1024/1024

            self.prev_system_out_size = system_output_size
            self.prev_system_in_size = system_in_size
            self.prev_measured_time = cur_time
            self.prev_system_out_bw = system_out_bw_instant
            self.prev_system_in_bw = system_in_bw_instant

            self.cur_system_in_bw_list.append((cur_time, system_in_bw_instant))
            self.cur_system_out_bw_list.append(
                (cur_time, system_out_bw_instant))
            #system_in_bw_actual = self._averaged_value(self.cur_system_in_bw_list, cur_time, 1)
            #system_out_bw_actual = self._averaged_value(self.cur_system_out_bw_list, cur_time, 1)
            system_in_bw_actual = system_in_bw_instant
            system_out_bw_actual = system_out_bw_instant

        return p_dict, r_dict, p_dict_cur, r_dict_cur, \
            total_p, total_r, total_p_cur, total_r_cur, \
            total_size_dict_in, system_block_per_sec_cur, \
            system_out_bw_cur_est, system_in_bw_cur_est, \
            system_out_bw_actual, system_in_bw_actual

    def get_migration_iteration_count(self):
        worker = self.process_list.get("CreateMemoryDeltalist", None)
        if worker is None:
            return None
        iteration_num = worker.monitor_current_iteration.value
        return iteration_num

    def get_network_speed(self):
        if self.migration_dest.startswith("tcp"):
            # Only used for experiement.  If it's bigger than 0, adaptation use 
            # this value to transmit over the network
            if VMOverlayCreationMode.USE_STATIC_NETWORK_BANDWIDTH > 0:
                return VMOverlayCreationMode.USE_STATIC_NETWORK_BANDWIDTH
            elif VMOverlayCreationMode.USE_PUBSUB_NETWORK_BANDWIDTH:
                try:
                    topic, data = self.subscriber.recv_string(flags=zmq.NOBLOCK).split()
                    self.new_bandwidth_published = True
                    return float(data)
                except zmq.ZMQError:
                    self.new_bandwidth_published = False
                    return None
            else:
                worker = self.process_list.get("StreamSynthesisClient", None)
                if worker is None:
                    return None
                worker_info = self.process_infos["StreamSynthesisClient"]
                if worker_info['is_processing_alive'].value == False:
                    return None
                network_bw = worker.monitor_network_bw.value
                if network_bw <= 0:
                    return None
                return network_bw  # mbps
        else:
            return VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps

    def start_managing(self):
        self.time_start = time.time()
        self.prev_system_out_size = 0
        self.prev_system_in_size = 0
        self.prev_system_out_bw = 0
        self.prev_system_in_bw = 0
        self.prev_measured_time = self.time_start
        self.cur_system_in_bw_list = list()
        self.cur_system_out_bw_list = list()
        #LOG.debug("adaptation start time: %f" % self.time_start)
        time_first_measurement = 0
        mode_change_history = list()
        time_prev_mode_change = self.time_start
        while (not self.stop.wait(0.1)):
            try:
                network_bw = self.get_network_speed()  # mega bit/s
                system_speed = self.get_system_speed()
                time_current_iter = time.time()
                if system_speed is None:
                    #LOG.debug("system speed is not measured")
                    continue
                if network_bw is None:
                    #LOG.debug("network speed is not measured")
                    continue
                if time_first_measurement == 0:
                    time_first_measurement = time.time()
                time_from_start = time_current_iter-time_first_measurement

                p_dict, r_dict, p_dict_cur, r_dict_cur,\
                    total_p, total_r, total_p_cur, total_r_cur,\
                    total_size_dict_in, system_block_per_sec,\
                    system_out_bw_cur_est, system_in_bw_cur_est,\
                    system_out_bw_actual, system_in_bw_actual = system_speed
                msg = "[measurement]\t%0.2f\t%0.2f\t%0.4f\t%0.4f\t%0.4f\t%0.4f" % \
                    (time_from_start,
                     network_bw,
                     total_p, total_r,
                     total_p_cur, total_r_cur)
                LOG.debug(msg)

                # first predict at 2 seconds and then for every 5 seconds
                if  (VMOverlayCreationMode.USE_PUBSUB_NETWORK_BANDWIDTH and self.new_bandwidth_published) or /
                    (time_from_start > 5 and (time_current_iter-time_prev_mode_change) > 5):
                    LOG.debug("[measurement]\ttime\tbw\tavgP\tavgR\tcurrP\tcurrR")
                    item = self.mode_profile.predict_new_mode(
                        self.overlay_creation_mode,
                        p_dict_cur,
                        r_dict_cur,
                        total_size_dict_in,
                        network_bw)
                    diff_mode = None
                    if item is not None:
                        (new_mode_obj, actual_block_per_sec, misc) = item
                        (bottleneck,
                         system_block_per_sec,
                         new_system_in_mbps,
                         new_system_out_mbps,
                         network_block_per_sec,
                         network_bw) = misc
                        diff_str = MigrationMode.mode_diff_str(
                            self.overlay_creation_mode.__dict__,
                            new_mode_obj.mode)
                        diff_mode = MigrationMode.mode_diff(
                            self.overlay_creation_mode.__dict__,
                            new_mode_obj.mode)

                    # change mode
                    time_prev_mode_change = time_current_iter
                    if diff_mode is not None and len(diff_mode) > 0:
                        new_comp_level = None
                        new_comp_type = None
                        new_disk_diff = None
                        new_memory_diff = None
                        if "COMPRESSION_ALGORITHM_SPEED" in diff_mode.keys():
                            new_comp_level = diff_mode[
                                "COMPRESSION_ALGORITHM_SPEED"]
                        if "COMPRESSION_ALGORITHM_TYPE" in diff_mode.keys():
                            new_comp_type = diff_mode[
                                "COMPRESSION_ALGORITHM_TYPE"]
                        if "DISK_DIFF_ALGORITHM" in diff_mode.keys():
                            new_disk_diff = diff_mode["DISK_DIFF_ALGORITHM"]
                        if "MEMORY_DIFF_ALGORITHM" in diff_mode.keys():
                            new_memory_diff = diff_mode[
                                "MEMORY_DIFF_ALGORITHM"]

                        # apply change
                        if new_comp_type is not None or new_comp_level is not None:
                            self._change_comp_mode(new_comp_type,
                                                   new_comp_level)
                        if new_disk_diff is not None:
                            self._change_disk_diff_mode(new_disk_diff)
                        if new_memory_diff is not None:
                            self._change_memory_diff_mode(new_memory_diff)

                        old_mode_dict = self.overlay_creation_mode.__dict__.copy()
                        self.overlay_creation_mode.update_mode(
                            new_mode_obj.mode)
                        mode_change_history.append(
                            (time_current_iter, old_mode_dict, new_mode_obj.mode)
                        )

                        # print log
                        diff_str = MigrationMode.mode_diff_str(
                            old_mode_dict,
                            self.overlay_creation_mode.__dict__)
                        LOG.debug("[adaptation]\t%0.2f\t%s\t%s\t%s\t%s" %
                                  (time_from_start,
                                   diff_str,
                                   p_dict_cur,
                                   r_dict_cur,
                                   total_size_dict_in))

                    else:
                        LOG.debug(
                            "[adaptation]\t%0.2f\tcurrent mode is the best" %
                            ( time_from_start))
            except Exception as e:
                sys.stdout.write("[manager] Exception\n")
                sys.stderr.write(traceback.format_exc())
                sys.stderr.write("%s\n" % str(e))
                sys.stdout.write("[manager] Exception\n")

    def register(self, worker):
        worker_name = getattr(worker, "worker_name", "NoName")
        worker_info = dict()
        worker_info['is_processing_alive'] = worker.is_processing_alive
        worker_info['finish_processing_input'] = worker.finish_processing_input
        control_queue = multiprocessing.Queue()
        response_queue = multiprocessing.Queue()
        # print "[manager] register new process: %s" % worker_name

        self.process_list[worker_name] = worker
        self.process_infos[worker_name] = (worker_info)
        self.process_control[worker_name] = (control_queue, response_queue)
        return control_queue, response_queue

    def terminate(self):
        self.stop.set()


class ProcWorker(multiprocessing.Process):

    def __init__(self, *args, **kwargs):
        # measurement
        self.monitor_total_time_block = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.monitor_total_ratio_block = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.monitor_total_time_block_cur = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.monitor_total_ratio_block_cur = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.monitor_total_input_size = multiprocessing.RawValue(
            ctypes.c_ulong, 0)
        self.monitor_total_output_size = multiprocessing.RawValue(
            ctypes.c_ulong, 0)
        self.monitor_total_input_size_cur = multiprocessing.RawValue(
            ctypes.c_ulong, 0)
        self.monitor_total_output_size_cur = multiprocessing.RawValue(
            ctypes.c_ulong, 0)
        self.in_size = 0
        self.out_size = 0
        self.is_processing_alive = multiprocessing.RawValue(ctypes.c_bool)
        self.finish_processing_input = multiprocessing.RawValue(ctypes.c_bool)
        self.is_processing_alive.value = True
        self.finish_processing_input.value = False

        self.worker_name = str(
            kwargs.pop('worker_name',
                       self.__class__.__name__))
        process_manager = get_instance()
        (self.control_queue, self.response_queue) = \
            process_manager.register(self)  # shared dictionary

        # not used
        self.monitor_current_bw = float(0)
        self.monitor_current_inqueue_length = multiprocessing.Value('d', -1.0)
        self.monitor_current_outqueue_length = multiprocessing.Value('d', -1.0)
        self.monitor_current_get_time = multiprocessing.Value('d', -1.0)
        self.monitor_current_put_time = multiprocessing.Value('d', -1.0)
        super(ProcWorker, self).__init__(*args, **kwargs)

    def change_affinity_child(self, new_num_cores):
        for (proc, c_queue, m_queue) in self.proc_list:
            if proc.is_alive():
                m_queue.put(("new_num_cores", new_num_cores))

    def _handle_control_msg(self, control_msg):
        if control_msg == "current_bw":
            self.response_queue.put(self.monitor_current_bw)
            return True
        elif control_msg == "queue_length":
            return (
                self.monitor_current_inqueue_length,
                self.monitor_current_outqueue_length)
        elif control_msg == "change_cores":
            new_num_cores = self.control_queue.get()
            num_cores = new_num_cores.get("num_cores", None)
            if num_cores is not None:
                # print "[%s] itself receives new num cores: %s" % (self,
                # num_cores)
                VMOverlayCreationMode.set_num_cores(num_cores)
                if getattr(self, "proc_list", None):
                    self.change_affinity_child(num_cores)
            return True
        else:
            # sys.stdout.write("Cannot be handled in super class\n")
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
