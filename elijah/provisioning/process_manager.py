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

        # load profiling information
        path = VMOverlayCreationMode.PROFILE_DATAPATH
        super(ProcessManager, self).__init__(target=self.start_managing)

    def set_mode(self, new_mode):
        self.overlay_creation_mode = new_mode

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

    def _change_diff_mode(self, diff_algorithm):
        worker_names = self.process_list.keys()
        if "CreateMemoryDeltalist" in worker_names:
            self._send_query("change_mode",
                            ["CreateMemoryDeltalist"],
                            data={"diff_algorithm":diff_algorithm})
        if "CreateDiskDeltalist" in worker_names:
            self._send_query("change_mode",
                            ["CreateDiskDeltalist"],
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
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            if worker == None:
                #sys.stdout.write("pipe is not fully working yet\n")
                return
            process_info = self.process_infos[worker_name]
            #if process_info['is_alive'] == True:
            time_block = worker.monitor_total_time_block.value
            ratio_block = worker.monitor_total_ratio_block.value
            if time_block <= 0 or ratio_block <=0:
                #sys.stdout.write("pipe is not fully working yet\n")
                return
            p_dict[worker_name] = time_block
            r_dict[worker_name] = ratio_block

        # Get P and R
        time_cpu = max(p_dict['CreateDiskDeltalist'], p_dict['CreateMemoryDeltalist']) + p_dict['DeltaDedup'] + p_dict['CompressProc']
        throughput_per_cpu_per_block = 1/(time_cpu)
        throughput_per_cpu_MBps = (4096+11)*throughput_per_cpu_per_block/1024.0/1024
        throughput_cpus_MBps = self.overlay_creation_mode.NUM_PROC_COMPRESSION*throughput_per_cpu_MBps
        ratio = (0.5*r_dict['CreateDiskDeltalist'] + 0.5*r_dict['CreateMemoryDeltalist'])*r_dict['DeltaDedup']*r_dict['CompressProc']
        throughput_network_MBps = throughput_cpus_MBps*ratio
        #sys.stdout.write("CPU: %f MBps\tRatio:%f, Network: %f MBps\t(%f,%f), (%f,%f), (%f,%f), (%f,%f)\n" % \
        #                 (throughput_cpus_MBps,
        #                  ratio, throughput_network_MBps,
        #                  p_dict['CreateDiskDeltalist'],
        #                  r_dict['CreateDiskDeltalist'],
        #                  p_dict['CreateMemoryDeltalist'],
        #                  r_dict['CreateMemoryDeltalist'],
        #                  p_dict['DeltaDedup'],
        #                  r_dict['DeltaDedup'],
        #                  p_dict['CompressProc'],
        #                  r_dict['CompressProc']
        #                  ))

        return p_dict, r_dict, throughput_network_MBps*8.0

    def get_network_speed(self):
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

    def start_managing(self):
        time_s = time.time()
        self.cpu_statistics = list()
        mode_change_log = list()
        count = 0 
        while (not self.stop.wait(0.1)):
            try:
                network_bw_mbps = self.get_network_speed()  # mega bit/s
                system_speed = self.get_system_speed()
                if system_speed == None or network_bw_mbps == None:
                    sys.stdout.write("synthesis stream client is not working yet\n")
                    continue
                p_dict, r_dict, system_bw_mbps = system_speed
                print "system throughput : %f mbps\tnetwork throughput: %f mbps" % (system_bw_mbps, network_bw_mbps)

                '''
                if count == 100:
                    bw_diff = system_bw_mbps - network_bw_mbps
                    if bw_diff > 1: # network is bottleneck. Do more computation and
                        print "network is bottleneck"
                        #self._change_comp_mode(Const.COMPRESSION_LZMA, 9)
                        self._change_diff_mode("bsdiff")
                        mode_change_log.append("new")
                    elif bw_diff < -1:  # computation is bottlneck. Speed up computation
                        print "computing is bottleneck"
                        self._change_diff_mode("none")
                        #self._change_comp_mode(Const.COMPRESSION_GZIP, 1)
                        mode_change_log.append("new")
                    else:
                        # stable
                        print "current mode is stable"
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

