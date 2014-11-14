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
import multiprocessing
import threading
import time
import sys
import traceback


_process_controller = None



def get_instance():
    global _process_controller

    if _process_controller == None:
        _process_controller = ProcessManager()
        _process_controller.daemon = True
        _process_controller.start()
    return _process_controller



class ProcessManager(threading.Thread):
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.process_list = dict()
        self.process_infos = self.manager.dict()
        self.process_control = dict()
        self.stop = threading.Event()
        super(ProcessManager, self).__init__(target=self.start_managing)

    def _send_query(self, query, worker_names):
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            if worker.is_alive():
                control_queue.put(query)


    def _recv_response(self, query, worker_names):
        for worker_name in worker_names:
            worker = self.process_list.get(worker_name, None)
            control_queue, response_queue = self.process_control[worker_name]
            if worker.is_alive():
                response = response_queue.get()
                #print "[manager] %s, %s: %s" % (query, worker_name, response)


    def start_managing(self):
        try:
            while (not self.stop.wait(0.1)):
                pass
                ## send control query
                #query = "current_bw"
                #worker_names = self.process_list.keys()
                #self._send_query(query, worker_names)

                ## recv control response
                #responses = self._recv_response(query, worker_names)
        except Exception as e:
            sys.stdout.write("[manager] Exception")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))

    def register(self, worker):
        worker_name = getattr(worker, "worker_name", "NoName")
        process_info = self.manager.dict()
        process_info['update_period'] = 0.1 # seconds
        control_queue = multiprocessing.Queue()
        response_queue = multiprocessing.Queue()
        #print "[manager] register new process: %s" % worker_name

        self.process_list[worker_name] = worker
        self.process_infos[worker_name] = (process_info)
        self.process_control[worker_name] = (control_queue, response_queue)
        return process_info, control_queue, response_queue


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

