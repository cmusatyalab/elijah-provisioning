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
import argparse
import multiprocessing
import Queue
import msgpack

if os.path.exists("../provisioning"):
    sys.path.insert(0, "../../")
try:
    from elijah.provisioning.server import NetworkUtil
    from elijah.provisioning.Configuration import Const
    from elijah.provisioning.package import VMOverlayPackage
    from elijah.provisioning.synthesis_protocol import Protocol
except ImportError as e:
    sys.stderr.write(str(e))
    sys.exit(1)



class StreamSynthesisClient(multiprocessing.Process):
    EMULATED_BANDWIDTH_Mbps = 100000    # Mbps

    def __init__(self, metadata, compdata_queue):
        self.metadata = metadata
        self.compdata_queue = compdata_queue
        super(StreamSynthesisClient, self).__init__(target=self.transfer)

    def transfer(self):
        # connect
        address = ("127.0.0.1", 8022)
        print "Connecting to (%s).." % str(address)
        sock = socket.create_connection(address, 10)
        sock.setblocking(True)

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
                LOG.error("Failed to get compressed data")
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
            print "transfer: %d" % transfer_size
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
            emulated_time = (processed_size*8) / (self.EMULATED_BANDWIDTH_Mbps*1024.0*1024)
            if emulated_time > processed_time:
                sleep_time = (emulated_time-processed_time)
                sys.stdout.write("Emulating BW of %d Mbps, so wait %f s\n" %\
                        (self.EMULATED_BANDWIDTH_Mbps, sleep_time))
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
        sys.stdout.write("Finish\n")



def synthesize_data(overlay_path, comp_queue):
    overlay_package = VMOverlayPackage("file:///%s" %
                                       os.path.abspath(overlay_path))
    meta_raw = overlay_package.read_meta()
    meta_info = msgpack.unpackb(meta_raw)
    comp_overlay_files = meta_info[Const.META_OVERLAY_FILES]

    for blob_info in comp_overlay_files:
        comp_filename = blob_info[Const.META_OVERLAY_FILE_NAME]
        comp_type = blob_info.get(Const.META_OVERLAY_FILE_COMPRESSION, Const.COMPRESSION_LZMA)
        output_data = overlay_package.read_blob(comp_filename)
        modified_disk_chunks = blob_info.get(Const.META_OVERLAY_FILE_DISK_CHUNKS)
        modified_memory_chunks = blob_info.get(Const.META_OVERLAY_FILE_MEMORY_CHUNKS)
        comp_queue.put((comp_type, output_data, modified_disk_chunks, modified_memory_chunks))

    new_meta_info = dict()
    new_meta_info[Const.META_BASE_VM_SHA256] = meta_info[Const.META_BASE_VM_SHA256]
    new_meta_info[Const.META_RESUME_VM_DISK_SIZE] = meta_info[Const.META_RESUME_VM_DISK_SIZE]
    new_meta_info[Const.META_RESUME_VM_MEMORY_SIZE] = meta_info[Const.META_RESUME_VM_MEMORY_SIZE]
    comp_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
    return new_meta_info


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('overlay_file')
    args = parser.parse_args()

    comp_queue = Queue.Queue()
    metadata = synthesize_data(args.overlay_file, comp_queue)
    basevm_uuid = "406ed612a6a8b8a03fbbc5f45cceb0408a1c1d947f09d3b8a5352973d77d01f5"
    stream_client = StreamSynthesisClient(metadata, comp_queue)
    stream_client.start()
    stream_client.join()


if __name__ == "__main__":
    try:
        status = main()
        sys.exit(status)
    except KeyboardInterrupt:
        is_stop_thread = True
        sys.exit(1)
