#!/usr/bin/env python 
#
# cloudlet infrastructure for mobile computing
#
#   author: kiryong ha <krha@cmu.edu>
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

import socket
import os
import time
import sys
import struct
import threading
import multiprocessing
import msgpack
import ctypes

#if os.path.exists("../provisioning"):
#    sys.path.insert(0, "../../")
#try:
#    from elijah.provisioning.server import NetworkUtil
#    from elijah.provisioning.configuration import Const
#    from elijah.provisioning.package import VMOverlayPackage
#    from elijah.provisioning.synthesis_protocol import Protocol
#except ImportError as e:
#    sys.stderr.write("%s\n" % str(e))
#    sys.exit(1)
from server import NetworkUtil
from configuration import Const
from configuration import VMOverlayCreationMode
from synthesis_protocol import Protocol
import process_manager
import logging
from .db.api import update_op, log_op

LOG = logging.getLogger(__name__)


ACK_DATA_SIZE = 100*1024


class StreamSynthesisClientError(Exception):
    pass

class NetworkMeasurementThread(threading.Thread):
    def __init__(self, sock, blob_sent_time_dict, monitor_network_bw, vm_resume_time_at_dest):
        self.sock = sock
        self.blob_sent_time_dict = blob_sent_time_dict

        # shared memory
        self.monitor_network_bw = monitor_network_bw
        self.vm_resume_time_at_dest = vm_resume_time_at_dest
        threading.Thread.__init__(self, target=self.receiving)

    @staticmethod
    def time_average(measure_history, start_time, cur_time):
        sum_value = float(0)
        counter = 0
        for (measured_time, value) in reversed(measure_history):
            # average only last 2s from current and after 3 seconds since start
            #if (cur_time - measured_time) > 2 or (measured_time - start_time) < 3:
            if (measured_time - start_time) < 3:
                break
            if counter > 5:
                break
            sum_value += value
            counter += 1
        if len(measure_history) > 2:
            #LOG.debug("bandwidth\t%f\t%f\t%0.4f --> %0.4f\t%d/%d" % (cur_time,
            #                                                         start_time,
            #                                                         measure_history[-2][-1],
            #                                                         measure_history[-1][-1],
            #                                                         counter,
            #                                                         len(measure_history)))
            pass
        if counter == 0:
            return measure_history[-1][1]
        return sum_value/counter


    def receiving(self):
        ack_time_list = list()
        measured_bw_list = list()
        ack_size = 8
        time_start = 0
        measured_bw_list = list()
        while True:
            ack_data = self.sock.recv(ack_size)
            ack = struct.unpack("!Q", ack_data)[0]
            time_recv_prev = time.time()
            if (ack == 0x01):
                # start receiving acks of new blob
                measure_bw_blob = list()
                while True:
                    ack_data = self.sock.recv(ack_size)
                    ack_recved_data = struct.unpack("!Q", ack_data)[0]
                    if ack_recved_data == 0x02:
                        break
                    if time_start == 0:
                        time_start = time.time()
                    time_recv_cur = time.time()
                    receive_duration = time_recv_cur - time_recv_prev
                    bw_mbps = 8*ack_recved_data/receive_duration/1024.0/1024
                    #print "ack: %f, %f, %f, %ld, %f mbps" % (
                    #    time_recv_cur,
                    #    time_recv_prev,
                    #    receive_duration,
                    #    ack_recved_data,
                    #    bw_mbps)
                    time_recv_prev = time_recv_cur

                    # filter out error bw
                    if len(measured_bw_list) > 0:
                        prev_bw = measured_bw_list[-1][-1]
                        #LOG.debug("bandwidth\t%f\tvalue change: %f --> %f" % (time_recv_cur, prev_bw, bw_mbps))
                        # assume error for one-order of magnitude changes
                        if bw_mbps > prev_bw*10 or bw_mbps < prev_bw*0.1 or\
                                bw_mbps > 1024:
                            #LOG.debug("bandwidth\t%f\tfiltered value: %f --> %f" % (time_recv_cur, prev_bw, bw_mbps))
                            bw_mbps = prev_bw
                    measure_bw_blob.append(bw_mbps)
                if len(measure_bw_blob) > 0:
                    median_bw = measure_bw_blob[len(measure_bw_blob)/2]
                    measured_bw_list.append((time_recv_cur, median_bw))
                self.monitor_network_bw.value = self.time_average(measured_bw_list,
                                                                  time_start,
                                                                  time_recv_cur)
            elif (ack == 0x10):
                data = self.sock.recv(8)
                vm_resume_time = struct.unpack("!d", data)[0]
                self.vm_resume_time_at_dest.value = float(vm_resume_time)
                print "migration resume time: %f" % (vm_resume_time)
                break
            else:
                pass


class StreamSynthesisClient(process_manager.ProcWorker):

    def __init__(self, remote_addr, remote_port, metadata, compdata_queue, process_controller, operation_id):
        self.remote_addr = remote_addr
        self.remote_port = remote_port
        self.metadata = metadata
        self.compdata_queue = compdata_queue
        self.process_controller = process_controller
        self.operation_id = operation_id

        # measurement
        self.monitor_network_bw = multiprocessing.RawValue(ctypes.c_double, 0)
        self.monitor_network_bw.value = 0.0
        self.vm_resume_time_at_dest = multiprocessing.RawValue(ctypes.c_double, 0)
        self.time_finish_transmission = multiprocessing.RawValue(ctypes.c_double, 0)

        self.is_first_recv = False
        self.time_first_recv = 0

        super(StreamSynthesisClient, self).__init__(target=self.transfer)

    def transfer(self):
        # connect
        address = (self.remote_addr, self.remote_port)
        sock = None
        for index in range(5):
            LOG.info("Connecting to (%s).." % str(address))
            try:
                sock = socket.create_connection(address, 10)
                break
            except Exception as e:
                time.sleep(1)
                LOG.error(e)
        if sock == None:
            msg = "failed to connect to %s" % str(address)
            raise StreamSynthesisClientError(msg)
        sock.setblocking(True)
        self.blob_sent_time_dict = dict()
        self.receive_thread = NetworkMeasurementThread(sock,
                                                       self.blob_sent_time_dict,
                                                       self.monitor_network_bw,
                                                       self.vm_resume_time_at_dest)
        self.receive_thread.start()

        # send header
        header_dict = {
            Protocol.KEY_SYNTHESIS_OPTION: None,
            }
        header_dict.update(self.metadata)
        header = NetworkUtil.encoding(header_dict)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)

        # stream blob
        blob_counter = 0
        while True:
            comp_task = self.compdata_queue.get()
            if self.is_first_recv == False:
                self.is_first_recv = True
                self.time_first_recv = time.time()
                LOG.debug("[time] Transfer first input at : %f" % (self.time_first_recv))
            transfer_size = 0
            if comp_task == Const.QUEUE_SUCCESS_MESSAGE:
                break
            if comp_task == Const.QUEUE_FAILED_MESSAGE:
               LOG.error("Failed to get compressed data!")
                break
            (blob_comp_type, compdata, disk_chunks, memory_chunks) = comp_task
            blob_header_dict = {
                Const.META_OVERLAY_FILE_COMPRESSION: blob_comp_type,
                Const.META_OVERLAY_FILE_SIZE:len(compdata),
                Const.META_OVERLAY_FILE_DISK_CHUNKS: disk_chunks,
                Const.META_OVERLAY_FILE_MEMORY_CHUNKS: memory_chunks
                }
            # send
            header = NetworkUtil.encoding(blob_header_dict)
            sock.sendall(struct.pack("!I", len(header)))
            sock.sendall(header)
            self.blob_sent_time_dict[blob_counter] = (time.time(), len(compdata))
            sock.sendall(compdata)
            transfer_size += (4+len(header)+len(compdata))
            blob_counter += 1
            #send the current iteration number for use at the destination
            sock.sendall(struct.pack("!I", self.process_controller.get_migration_iteration_count()))
            update_op(self.operation_id, notes="Blobs sent: %d, Iteration: %d" % (blob_counter, self.process_controller.get_migration_iteration_count()))

        # end message
        end_header = {
            "blob_type": "blob",
            Const.META_OVERLAY_FILE_SIZE:0
        }
        header = NetworkUtil.encoding(end_header)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)

        self.is_processing_alive.value = False
        self.time_finish_transmission.value = time.time()
        sys.stdout.write("Finish transmission. Waiting for finishing migration\n")
        self.receive_thread.join()
        sock.close()
