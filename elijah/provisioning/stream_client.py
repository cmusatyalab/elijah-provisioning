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
            "basevm_uuid": self.basevm_uuid,
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
        end_header = {
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
        comp_queue.put((comp_type, output_data, modified_disk_chunks, modified_memory_chunks))
    comp_queue.put(Const.QUEUE_SUCCESS_MESSAGE)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('overlay_file')
    args = parser.parse_args()

    comp_queue = Queue.Queue()
    synthesize_data(args.overlay_file, comp_queue)
    stream_client = StreamSynthesisClient("asdasd", comp_queue)
    stream_client.start()


if __name__ == "__main__":
    try:
        status = main()
        sys.exit(status)
    except KeyboardInterrupt:
        is_stop_thread = True
        sys.exit(1)
