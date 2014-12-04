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
#    from elijah.provisioning.Configuration import Const
#    from elijah.provisioning.package import VMOverlayPackage
#    from elijah.provisioning.synthesis_protocol import Protocol
#except ImportError as e:
#    sys.stderr.write("%s\n" % str(e))
#    sys.exit(1)
from server import NetworkUtil
from Configuration import Const
from Configuration import VMOverlayCreationMode
from synthesis_protocol import Protocol
import process_manager
import log as logging

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

    def receiving(self):
        ack_time_list = list()
        measured_bw_list = list()
        ack_size = 8
        while True:
            ack_data = self.sock.recv(ack_size)
            ack = struct.unpack("!Q", ack_data)[0]
            time_recv_prev = time.time()
            averaged_bw_list = list()
            if (ack == 0x01):
                # start receiving acks of new blob
                while True:
                    ack_data = self.sock.recv(ack_size)
                    ack_recved_data = struct.unpack("!Q", ack_data)[0] 
                    if ack_recved_data == 0x02:
                        break
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
                    averaged_bw_list.append(bw_mbps)
                if len(averaged_bw_list) > 0:
                    averaged_bw = sum(averaged_bw_list)/len(averaged_bw_list)
                    #print "average bw: %f" % averaged_bw

                self.monitor_network_bw.value = averaged_bw
            elif (ack == 0x10):
                data = self.sock.recv(8)
                vm_resume_time = struct.unpack("!d", data)[0]
                self.vm_resume_time_at_dest.value = float(vm_resume_time)
                print "migration resume time: %f" % vm_resume_time
                break
            else:
                print "error"
                pass


class StreamSynthesisClient(process_manager.ProcWorker):

    def __init__(self, remote_addr, metadata, compdata_queue):
        self.remote_addr = remote_addr
        self.metadata = metadata
        self.compdata_queue = compdata_queue

        # measurement
        self.monitor_network_bw = multiprocessing.RawValue(ctypes.c_double, 0)
        self.monitor_network_bw.value = 0.0
        self.vm_resume_time_at_dest = multiprocessing.RawValue(ctypes.c_double, 0)

        self.is_first_recv = False
        self.time_first_recv = 0

        super(StreamSynthesisClient, self).__init__(target=self.transfer)

    def transfer(self):
        # connect
        address = (self.remote_addr, 8022)
        #address = ("128.2.213.12", 8022)
        print "Connecting to (%s).." % str(address)
        sock = socket.create_connection(address, 10)
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
            time_process_start = time.time()
            transfer_size = 0
            if comp_task == Const.QUEUE_SUCCESS_MESSAGE:
                break
            if comp_task == Const.QUEUE_FAILED_MESSAGE:
                sys.stderr.write("Failed to get compressed data\n")
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
            #print "transfer: %d" % transfer_size

            # wait to emulate network badwidth
            #time_process_end = time.time()
            #processed_time = time_process_end-time_process_start
            #processed_size = transfer_size
            #emulated_time = (processed_size*8) / (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps*1024.0*1024)
            #if emulated_time > processed_time:
            #    sleep_time = (emulated_time-processed_time)
            #    #sys.stdout.write("Emulating BW of %d Mbps, so wait %f s\n" %\ (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps, sleep_time))
            #    time.sleep(sleep_time)

        # end message
        end_header = {
            "blob_type": "blob",
            Const.META_OVERLAY_FILE_SIZE:0
        }
        header = NetworkUtil.encoding(end_header)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)

        self.is_processing_alive.value = False
        sys.stdout.write("Finish transmission. Waiting for finishing migration\n")
        self.receive_thread.join()
        sock.close()
