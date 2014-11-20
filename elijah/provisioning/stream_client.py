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



class StreamSynthesisClient(object):
    def __init__(self, basevm_uuid, compdata_queue):
        self.basevm_uuid = basevm_uuid
        self.compdata_queue = compdata_queue

    def start(self):
        # connect
        address = ("127.0.0.1", 8022)
        print "Connecting to (%s).." % str(address)
        sock = socket.create_connection(address, 10)
        sock.setblocking(True)

        # send header
        header_dict = {
            Const.META_BASE_VM_SHA256: self.basevm_uuid,
            Protocol.KEY_SYNTHESIS_OPTION: None,
            }
        header = NetworkUtil.encoding(header_dict)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)

        # stream blob
        blob_counter = 0
        while True:
            comp_task = self.compdata_queue.get()
            if comp_task == Const.QUEUE_SUCCESS_MESSAGE:
                break
            if comp_task == Const.QUEUE_FAILED_MESSAGE:
                LOG.error("Failed to get compressed data")
                break
            blob_type = comp_task[0]
            if blob_type == "blob":
                (blob_comp_type, compdata, disk_chunks, memory_chunks) = comp_task[1:]
                blob_header_dict = {
                    "blob_type": "blob",
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

        # end message
        end_header = {
            "blob_type": "blob",
            Const.META_OVERLAY_FILE_SIZE:0
        }
        header = NetworkUtil.encoding(end_header)
        sock.sendall(struct.pack("!I", len(header)))
        sock.sendall(header)
        sock.close()


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
        comp_queue.put(("blob", comp_type, output_data, modified_disk_chunks, modified_memory_chunks))

    new_meta_info = dict()
    new_meta_info[Const.META_RESUME_VM_DISK_SIZE] = meta_info[Const.META_RESUME_VM_DISK_SIZE]
    new_meta_info[Const.META_RESUME_VM_MEMORY_SIZE] = meta_info[Const.META_RESUME_VM_MEMORY_SIZE]
    comp_queue.put(("meta", new_meta_info))
    comp_queue.put(Const.QUEUE_SUCCESS_MESSAGE)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('overlay_file')
    args = parser.parse_args()

    comp_queue = Queue.Queue()
    synthesize_data(args.overlay_file, comp_queue)
    basevm_uuid = "406ed612a6a8b8a03fbbc5f45cceb0408a1c1d947f09d3b8a5352973d77d01f5"
    stream_client = StreamSynthesisClient(basevm_uuid, comp_queue)
    stream_client.start()


if __name__ == "__main__":
    try:
        status = main()
        sys.exit(status)
    except KeyboardInterrupt:
        is_stop_thread = True
        sys.exit(1)
