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


class StreamSynthesisClientError(Exception):
    pass

class NetworkMeasurementThread(threading.Thread):
    def __init__(self, sock):
        self.sock = sock
        threading.Thread.__init__(target=self.receiving)

    def receiving(self):
        ack_size = 8
        while True:
            time_start_waiting = time.time()
            ack = self.recv_all(self.sock, ack_size)
            if len(ack) != ack_size:
                print "lost connection"
                break
            time_ack_received = time.time()
            print "ack received"

    def recv_all(self, sock, recv_size):
        data = ''
        while len(data) < recv_size:
            tmp_data = sock.recv(recv_size - len(data))
            if len(tmp_data) == 0:
                break
            data += tmp_data
        return data


class StreamSynthesisClient(multiprocessing.Process):
    EMULATED_BANDWIDTH_Mbps = 10 # Mbps

    def __init__(self, metadata, compdata_queue):
        self.metadata = metadata
        self.compdata_queue = compdata_queue
        super(StreamSynthesisClient, self).__init__(target=self.transfer)

    def transfer(self):
        # connect
        address = ("127.0.0.1", 8022)
        #address = ("128.2.213.12", 8022)
        print "Connecting to (%s).." % str(address)
        sock = socket.create_connection(address, 10)
        sock.setblocking(True)
        self.receive_thread = NetworkMeasurementThread(sock)
        self.receive_thread.daemon = True
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
            blob_counter += 1
            # send
            header = NetworkUtil.encoding(blob_header_dict)
            sock.sendall(struct.pack("!I", len(header)))
            sock.sendall(header)
            sock.sendall(compdata)
            transfer_size += (4+len(header)+len(compdata))
            #print "transfer: %d" % transfer_size
            '''
            elif blob_type == "meta":
                (metadata) = comp_task[1]
                disk_size = metadata[Const.META_RESUME_VM_DISK_SIZE]
                memory_size = metadata[Const.META_RESUME_VM_MEMORY_SIZE]
                blob_header_dict = {
                    "blob_type": "meta",
                    Const.META_RESUME_VM_DISK_SIZE: disk_size,
                    Const.META_RESUME_VM_MEMORY_SIZE: memory_size
                    }
                # send
                header = NetworkUtil.encoding(blob_header_dict)
                sock.sendall(struct.pack("!I", len(header)))
                sock.sendall(header)
                transfer_size += (4+len(header))
            '''

            # wait to emulate network badwidth
            time_process_end = time.time()
            processed_time = time_process_end-time_process_start
            processed_size = transfer_size
            emulated_time = (processed_size*8) / (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps*1024.0*1024)
            if emulated_time > processed_time:
                sleep_time = (emulated_time-processed_time)
                sys.stdout.write("Emulating BW of %d Mbps, so wait %f s\n" %\
                        (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps, sleep_time))
                time.sleep(sleep_time)

        # end message
        end_header = {
            "blob_type": "blob",
            Const.META_OVERLAY_FILE_SIZE:0
        }
        header = NetworkUtil.encoding(end_header)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)
        sock.close()
        #sys.stdout.write("Finish\n")

