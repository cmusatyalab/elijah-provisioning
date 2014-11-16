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
import sys
import traceback
import Queue
from Configuration import Const


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
        super(ProcessManager, self).__init__(target=self.start_managing)

    def set_mode(self, new_mode):
        self.overlay_creation_mode = new_mode

    def _send_query(self, query, worker_names, data=None):
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            process_info = self.process_infos[worker_name]
            if process_info['is_alive'] == True:
                control_queue.put(query)
                if data is not None:
                    control_queue.put(data)
            else:
                sys.stdout.write("not sending query to %s since it's over" %\
                                 worker_name)

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

    def _change_comp_mode(self):
        worker_names = self.process_list.keys()
        if "CompressProc" in worker_names:
            self._send_query("change_mode",
                            ["CompressProc"],
                            data={"comp_level":9, "comp_type":Const.COMPRESSION_BZIP2})

    def _change_diff_mode(self):
        worker_names = self.process_list.keys()
        #if "CreateMemoryDeltalist" in worker_names:
        #    self._send_query("change_mode",
        #                    ["CreateMemoryDeltalist"],
        #                    data={"diff_algorithm":"none"})
        if "CreateDiskDeltalist" in worker_names:
            self._send_query("change_mode",
                            ["CreateDiskDeltalist"],
                            data={"diff_algorithm":"none"})

    def _get_cpu_usage(self):
        result = dict()
        query = "cpu_usage_accum"   #"current_bw"
        worker_names = self.process_list.keys()
        self._send_query(query, worker_names)

        responses = self._recv_response(query, worker_names)
        for worker_name, (response, duration) in responses.iteritems():
            sys.stdout.write("[manager] %s:\t%s:\t%s\t(%f s)\n" % (query, worker_name, str(response), duration))
            result[worker_name] = response
        time_e = time.time()
        self.cpu_statistics.append((time.time()-time_s, result))
        sys.stdout.write("\n")

    def start_managing(self):
        time_s = time.time()
        self.cpu_statistics = list()
        try:
            while (not self.stop.wait(1)):
                #self._get_cpu_usage()
                time.sleep(1)
                #self._change_comp_mode()
                #self._change_diff_mode()
                break
        except Exception as e:
            sys.stdout.write("[manager] Exception")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))

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
        self.monitor_current_bw = float(0)
        self.monitor_current_inqueue_size = 0
        self.monitor_current_outqueue_size = 0
        super(ProcWorker, self).__init__(*args, **kwargs)

    def _handle_control_msg(self, control_msg):
        if control_msg == "current_bw":
            self.response_queue.put(self.monitor_current_bw)
            return True
        elif control_msg == "cpu_usage_accum":
            (utime, stime, child_utime, child_stime, elaspe_time) = os.times()
            all_times = utime+stime+child_utime+child_stime
            self.response_queue.put(float(all_times))
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

