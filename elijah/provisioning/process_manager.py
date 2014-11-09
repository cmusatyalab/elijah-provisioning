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
        self.process_infos = self.manager.dict()
        self.stop = threading.Event()
        super(ProcessManager, self).__init__(target=self.start_managing)

    def start_managing(self):
        while (not self.stop.wait(1)):
            pass
            #for worker_name in self.process_infos.keys():
            #    value = self.process_infos[worker_name]
            #print "monitoring: %s" % str(self.process_infos)

    def register(self, worker):
        process_info = self.manager.dict()
        process_info['update_period'] = 0.1 # seconds
        worker_name = getattr(worker, "worker_name", "NoName")
        self.process_infos[worker_name] = process_info
        return process_info


class ProcWorker(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        self.worker_name = str(kwargs.pop('worker_name', self.__class__.__name__))
        process_manager = get_instance()
        self.process_info = process_manager.register(self)  # shared dictionary
        super(ProcWorker, self).__init__(*args, **kwargs)


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

