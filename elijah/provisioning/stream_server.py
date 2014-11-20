#!/usr/bin/env python 
#
# Cloudlet Infrastructure for Mobile Computing
#
#   Author: Kiryong Ha <krha@cmu.edu>
#
#   Copyright (C) 2011-2013 Carnegie Mellon University
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import os
import traceback
import sys
import time
import struct
import SocketServer
import socket

import tempfile
import multiprocessing
import threading

import delta
from server import NetworkUtil
from synthesis_protocol import Protocol as Protocol
from synthesis import run_fuse
from synthesis import SynthesizedVM
from synthesis import connect_vnc

#import synthesis as synthesis
#from package import VMOverlayPackage
from db.api import DBConnector
from db.table_def import BaseVM
from Configuration import Const as Cloudlet_Const
from compression import DecompProc
from pprint import pformat
from optparse import OptionParser
import log as logging


LOG = logging.getLogger(__name__)
session_resources = dict()   # dict[session_id] = obj(SessionResource)


class StreamSynthesisError(Exception):
    pass


import mmap
import tool
from delta import DeltaItem
class RecoverDeltaProc(multiprocessing.Process):
    FUSE_INDEX_DISK = 1
    FUSE_INDEX_MEMORY = 2

    def __init__(self, base_disk, base_mem,
                 decomp_delta_queue, output_mem_path,
                 output_disk_path, chunk_size,
                 fuse_info_queue):
        if base_disk == None and base_mem == None:
            raise StreamSynthesisError("Need either base_disk or base_memory")

        self.decomp_delta_queue = decomp_delta_queue
        self.output_mem_path = output_mem_path
        self.output_disk_path = output_disk_path
        self.fuse_info_queue = fuse_info_queue
        self.base_disk = base_disk
        self.base_mem = base_mem

        self.base_disk_fd = None
        self.base_mem_fd = None
        self.raw_disk = None
        self.raw_mem = None
        self.mem_overlay_dict = None
        self.raw_mem_overlay = None
        self.chunk_size = chunk_size
        self.zero_data = struct.pack("!s", chr(0x00)) * chunk_size
        self.recovered_delta_dict = dict()
        self.delta_list = list()

        multiprocessing.Process.__init__(self, target=self.recover_deltaitem)

    def recover_deltaitem(self):
        time_start = time.time()

        # initialize reference data to use mmap
        count = 0
        self.base_disk_fd = open(self.base_disk, "rb")
        self.raw_disk = mmap.mmap(self.base_disk_fd.fileno(), 0, prot=mmap.PROT_READ)
        self.base_mem_fd = open(self.base_mem, "rb")
        self.raw_mem = mmap.mmap(self.base_mem_fd.fileno(), 0, prot=mmap.PROT_READ)
        self.recover_mem_fd = open(self.output_mem_path, "wrb")
        self.recover_disk_fd = open(self.output_disk_path, "wrb")

        unresolved_deltaitem_list = []
        while True:
            recv_data = self.decomp_delta_queue.get()
            if recv_data == Cloudlet_Const.QUEUE_SUCCESS_MESSAGE:
                break

            overlay_chunk_ids = list()
            # recv_data is a single blob so that it contains whole DeltaItem
            for delta_item in RecoverDeltaProc.from_stream(recv_data):
                ret = self.recover_item(delta_item)
                if ret == None:
                    # cannot find self reference point due to the parallel
                    # compression. Save this and do it later
                    unresolved_deltaitem_list.append(delta_item)
                    continue
                self.process_deltaitem(delta_item, overlay_chunk_ids)
                count += 1

            self.recover_mem_fd.flush()
            self.recover_disk_fd.flush()
            #self.fuse_info_queue.put(overlay_chunk_ids)

        LOG.info("[Delta] Handle dangling DeltaItem (%d)" % len(unresolved_deltaitem_list))
        overlay_chunk_ids = list()
        for delta_item in unresolved_deltaitem_list:
            ret = self.recover_item(delta_item)
            if ret == None:
                msg = "Cannot find self reference: type(%ld), offset(%ld), index(%ld)" % \
                        (delta_item.delta_type, delta_item.offset, delta_item.index)
                raise StreamSynthesisError(msg)
            self.process_deltaitem(delta_item, overlay_chunk_ids)
            count += 1

        self.recover_mem_fd.close()
        self.recover_mem_fd = None
        self.recover_disk_fd.close()
        self.recover_disk_fd = None
        #self.fuse_info_queue.put(overlay_chunk_ids)
        #self.fuse_info_queue.put(Cloudlet_Const.QUEUE_SUCCESS_MESSAGE)
        time_end = time.time()

        LOG.info("[time] Delta delta %ld chunks, (%s~%s): %s" % \
                (count, time_start, time_end, (time_end-time_start)))
        self.finish()

    def recover_item(self, delta_item):
        if type(delta_item) != DeltaItem:
            raise StreamSynthesisError("Need list of DeltaItem")

        #LOG.debug("recovering %ld/%ld" % (index, len(delta_list)))
        if (delta_item.ref_id == DeltaItem.REF_RAW):
            recover_data = delta_item.data
        elif (delta_item.ref_id == DeltaItem.REF_ZEROS):
            recover_data = self.zero_data
        elif (delta_item.ref_id == DeltaItem.REF_BASE_MEM):
            offset = delta_item.data
            recover_data = self.raw_mem[offset:offset+self.chunk_size]
        elif (delta_item.ref_id == DeltaItem.REF_BASE_DISK):
            offset = delta_item.data
            recover_data = self.raw_disk[offset:offset+self.chunk_size]
        elif delta_item.ref_id == DeltaItem.REF_SELF:
            ref_index = delta_item.data
            self_ref_delta_item = self.recovered_delta_dict.get(ref_index, None)
            if self_ref_delta_item == None:
                #msg = "Cannot find self reference: type(%ld), offset(%ld), index(%ld), ref_index(%ld)" % \
                #        (delta_item.delta_type, delta_item.offset, delta_item.index, ref_index)
                #raise StreamSynthesisError(msg)
                return None
            recover_data = self_ref_delta_item.data
        elif delta_item.ref_id == DeltaItem.REF_XDELTA:
            patch_data = delta_item.data
            patch_original_size = delta_item.offset_len
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY:
                base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
            elif delta_item.delta_type == DeltaItem.DELTA_DISK:
                base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
            else:
                raise StreamSynthesisError("Delta type should be either disk or memory")
            recover_data = tool.merge_data(base_data, patch_data, len(base_data)*5)
        else:
            raise StreamSynthesisError("Cannot recover: invalid referce id %d" % delta_item.ref_id)

        if len(recover_data) != delta_item.offset_len:
            msg = "Error, Recovered Size Error: %d, %d, ref_id: %s, data_len: %ld, offset: %ld, offset_len: %ld" % \
                    (delta_item.delta_type, len(recover_data), delta_item.ref_id, \
                    delta_item.data_len, delta_item.offset, delta_item.offset_len)
            print msg
            raise StreamSynthesisError(msg)

        # recover
        delta_item.ref_id = DeltaItem.REF_RAW
        delta_item.data = recover_data

        return delta_item


    @staticmethod
    def from_stream(data):
        cur_offset = 0
        while True:
            new_item, offset = RecoverDeltaProc.unpack_stream(data[cur_offset:])
            cur_offset += offset
            if len(data) < cur_offset:
                raise StopIteration()
            yield new_item

    @staticmethod
    def unpack_stream(stream, with_hashvalue=False):
        if len(stream) == 0:
            return None, 999999
        offset = 0
        data = stream[0:8+2+1]
        data_len = 0
        offset += (8+2+1)
        (ram_offset, offset_len, ref_info) = struct.unpack("!QHc", data)
        ref_id = ord(ref_info) & 0xF0
        delta_type = ord(ref_info) & 0x0F

        if ref_id == DeltaItem.REF_RAW or \
                ref_id == DeltaItem.REF_XDELTA:
            data_len = struct.unpack("!Q", stream[offset:offset+8])[0]
            offset += 8
            data = stream[offset:offset+data_len]
            offset += data_len
        elif ref_id == DeltaItem.REF_SELF:
            data = struct.unpack("!Q", stream[offset:offset+8])[0]
            offset += 8
        elif ref_id == DeltaItem.REF_BASE_DISK or \
                ref_id == DeltaItem.REF_BASE_MEM:
            data = struct.unpack("!Q", stream[offset:offset+8])[0]
            offset += 8

        # hash_value typically does not exist when recovered becuase we don't need it
        if with_hashvalue:
            # hash_value is only needed for residue case
            hash_value = struct.unpack("!32s", stream[offset:offset+32])[0]
            offset += 32
            item = DeltaItem(delta_type, ram_offset, offset_len, hash_value, ref_id, data_len, data)
        else:
            item = DeltaItem(delta_type, ram_offset, offset_len, None, ref_id, data_len, data)
        return item, offset

    def process_deltaitem(self, delta_item, overlay_chunk_ids):
        if len(delta_item.data) != delta_item.offset_len:
            msg = "recovered size is not same as page size, %ld != %ld" % \
                    (len(delta_item.data), delta_item.offset_len)
            raise StreamSynthesisError(msg)

        # save it to dictionary to find self_reference easily
        self.recovered_delta_dict[delta_item.index] = delta_item
        self.delta_list.append(delta_item)

        # write to output file 
        overlay_chunk_id = long(delta_item.offset/self.chunk_size)
        if delta_item.delta_type == DeltaItem.DELTA_MEMORY:
            self.recover_mem_fd.seek(delta_item.offset)
            self.recover_mem_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (RecoverDeltaProc.FUSE_INDEX_MEMORY, overlay_chunk_id))
        elif delta_item.delta_type == DeltaItem.DELTA_DISK:
            self.recover_disk_fd.seek(delta_item.offset)
            self.recover_disk_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (RecoverDeltaProc.FUSE_INDEX_DISK, overlay_chunk_id))

    def finish(self):
        if self.base_disk_fd is not None:
            self.base_disk_fd.close()
            self.base_disk_fd = None
        if self.base_mem_fd is not None:
            self.base_mem_fd.close()
            self.base_mem_fd = None
        if self.raw_disk is not None:
            self.raw_disk.close()
            self.raw_disk = None
        if self.raw_mem is not None:
            self.raw_mem.close()
            self.raw_mem = None
        if self.raw_mem_overlay is not None:
            self.raw_mem_overlay.close()
            self.raw_mem_overlay = None


class FuseFeedingProc(multiprocessing.Process):
    def __init__(self, fuse, fuse_info_queue):
        self.fuse = fuse
        self.fuse_info_queue = fuse_info_queue
        self.stop = threading.Event()
        multiprocessing.Process.__init__(self, target=self.feeding_thread)

    def feeding_thread(self):
        time_start = time.time()
        while True:
            self._running = True
            try:
                chunks = self.fuse_info_queue.get()
                if chunks == Cloudlet_Const.QUEUE_SUCCESS_MESSAGE:
                    break
                chunk_str = ','.join(chunks)
                #sys.stdout.write(chunk_str)
                self.fuse.fuse_write(chunk_str)
            except EOFError:
                break

        time_end = time.time()
        LOG.info("[time] [FUSE] %s ~ %s: %s\n" % \
                (time_start, time_end, (time_end-time_start)))
        self.fuse.fuse_write("END_OF_TRANSMISSION")

    def terminate(self):
        self.stop.set()

    def terminate(self):
        self.stop.set()


class StreamSynthesisHandler(SocketServer.StreamRequestHandler):
    synthesis_option = {
            Protocol.SYNTHESIS_OPTION_DISPLAY_VNC : False,
            Protocol.SYNTHESIS_OPTION_EARLY_START : False,
            Protocol.SYNTHESIS_OPTION_SHOW_STATISTICS : False
            }

    def ret_fail(self, message):
        LOG.error("%s" % str(message))
        message = NetworkUtil.encoding({
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_FAIELD,
            Protocol.KEY_FAILED_REASON : message
            })
        message_size = struct.pack("!I", len(message))
        self.request.send(message_size)
        self.wfile.write(message)

    def ret_success(self, req_command, payload=None):
        send_message = {
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_SUCCESS,
            Protocol.KEY_REQUESTED_COMMAND : req_command,
            }
        if payload:
            send_message.update(payload)
        message = NetworkUtil.encoding(send_message)
        message_size = struct.pack("!I", len(message))
        self.request.send(message_size)
        self.wfile.write(message)
        self.wfile.flush()

    def send_synthesis_done(self):
        message = NetworkUtil.encoding({
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_SYNTHESIS_DONE,
            })
        LOG.info("SUCCESS to launch VM")
        try:
            message_size = struct.pack("!I", len(message))
            self.request.send(message_size)
            self.wfile.write(message)
        except socket.error as e:
            pass

    def _recv_all(self, recv_size):
        data = ''
        while len(data) < recv_size:
            tmp_data = self.request.recv(recv_size - len(data))
            if tmp_data == None:
                raise StreamSynthesisError("Cannot recv data at %s" % str(self))
            if len(tmp_data) == 0:
                raise StreamSynthesisError("Recv 0 data at %s" % str(self))
            data += tmp_data
        return data

    def _check_validity(self, message):
        header_info = None
        requested_base = None

        synthesis_option = message.get(Protocol.KEY_SYNTHESIS_OPTION, None)
        base_hashvalue = message.get(Cloudlet_Const.META_BASE_VM_SHA256, None)

        # check base VM
        for each_basevm in self.server.basevm_list:
            if base_hashvalue == each_basevm.hash_value:
                LOG.info("New client request %s VM" \
                        % (each_basevm.disk_path))
                requested_base = each_basevm.disk_path
        return [synthesis_option, requested_base]

    def handle(self):
        '''Handle request from the client
        Each request follows this format:

        | header size | header | blob header size | blob header | blob data  |
        |  (4 bytes)  | (var)  | (4 bytes)        | (var bytes) | (var bytes)|
        '''
        # get header
        data = self.request.recv(4)
        if data == None or len(data) != 4:
            raise StreamSynthesisError("Failed to receive first byte of header")
        message_size = struct.unpack("!I", data)[0]
        msgpack_data = self._recv_all(message_size)
        message = NetworkUtil.decoding(msgpack_data)
        synthesis_option, base_diskpath = self._check_validity(message)
        if base_diskpath == None:
            raise StreamSynthesisError("No matching base VM")
        (base_diskmeta, base_mempath, base_memmeta) = \
                Cloudlet_Const.get_basepath(base_diskpath, check_exist=True)
        LOG.info("  - %s" % str(pformat(self.synthesis_option)))
        LOG.info("  - Base VM     : %s" % base_diskpath)

        # variables for FUSE
        temp_synthesis_dir = tempfile.mkdtemp(prefix="cloudlet-comp-")
        launch_disk_size = 0
        launch_memory_size = 0
        launch_disk = os.path.join(temp_synthesis_dir, "launch-disk")
        launch_mem = os.path.join(temp_synthesis_dir, "launch-mem")
        disk_chunk_list = list()
        memory_chunk_list = list()

        # start pipelining processes
        network_out_queue = multiprocessing.Queue()
        decomp_queue = multiprocessing.Queue()
        fuse_info_queue = multiprocessing.Queue()
        decomp_proc = DecompProc(network_out_queue, decomp_queue)
        decomp_proc.start()
        delta_proc = RecoverDeltaProc(base_diskpath, base_mempath,
                                      decomp_queue,
                                      launch_mem,
                                      launch_disk,
                                      Cloudlet_Const.CHUNK_SIZE,
                                      fuse_info_queue)
        delta_proc.start()

        # get each blob
        while True:
            data = self.request.recv(4)
            if data == None or len(data) != 4:
                raise StreamSynthesisError("Failed to receive first byte of header")
                break
            blob_header_size = struct.unpack("!I", data)[0]
            blob_header_raw = self._recv_all(blob_header_size)
            blob_header = NetworkUtil.decoding(blob_header_raw)
            blob_type = blob_header.get("blob_type", None)
            if blob_type == "blob":
                blob_size = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_SIZE)
                if blob_size == None:
                    raise StreamSynthesisError("Failed to receive blob")
                if blob_size == 0:
                    print "end of stream"
                    break
                blob_comp_type = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_COMPRESSION)
                blob_disk_chunk = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_DISK_CHUNKS)
                blob_memory_chunk = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_MEMORY_CHUNKS)
                disk_chunk_list += ["%ld:1" % item for item in blob_disk_chunk]
                memory_chunk_list += ["%ld:1" % item for item in blob_memory_chunk]
                compressed_blob = self._recv_all(blob_size)
                network_out_queue.put((blob_comp_type, compressed_blob))
            elif blob_type == "meta":
                print blob_header
                launch_disk_size = blob_header[Cloudlet_Const.META_RESUME_VM_DISK_SIZE]
                launch_memory_size = blob_header[Cloudlet_Const.META_RESUME_VM_MEMORY_SIZE]
            else:
                raise StreamSynthesisError("need blob type")
        network_out_queue.put(Cloudlet_Const.QUEUE_SUCCESS_MESSAGE)
        delta_proc.join()
        # We told FUSE that we have everything ready, so we need to wait until
        # delta_proc fininshes
        # we cannot start VM before delta_proc finishes, because we don't know
        # what will be modified in the future

        time_fuse_start = time.time()
        disk_overlay_map = ','.join(disk_chunk_list)
        memory_overlay_map = ','.join(memory_chunk_list)
        fuse = run_fuse(Cloudlet_Const.CLOUDLETFS_PATH, Cloudlet_Const.CHUNK_SIZE,
                base_diskpath, launch_disk_size, base_mempath, launch_memory_size,
                resumed_disk=launch_disk,  disk_overlay_map=disk_overlay_map,
                resumed_memory=launch_mem, memory_overlay_map=memory_overlay_map)
        synthesized_VM = SynthesizedVM(launch_disk, launch_mem, fuse)
        #fuse_proc = FuseFeedingProc(fuse, fuse_info_queue)
        #fuse_proc.start()
        #fuse_proc.join()

        synthesized_VM.resume()
        time_fuse_end = time.time()
        LOG.info("[time] last, but non-pipelined time (%f ~ %f): %f" % (time_fuse_start,
                                                                        time_fuse_end,
                                                                        (time_fuse_end-time_fuse_start)))
        connect_vnc(synthesized_VM.machine)

        # terminate
        synthesized_VM.monitor.terminate()
        synthesized_VM.monitor.join()
        synthesized_VM.terminate()

    def terminate(self):
        # force terminate when something wrong in handling request
        # do not wait for joinining
        if hasattr(self, 'detla_proc') and self.delta_proc != None:
            self.delta_proc.finish()
            if self.delta_proc.is_alive():
                self.delta_proc.terminate()
            self.delta_proc = None
        if hasattr(self, 'resumed') and self.resumed_VM != None:
            self.resumed_VM.terminate()
            self.resumed_VM = None
        if hasattr(self, 'fuse') and self.fuse != None:
            self.fuse.terminate()
            self.fuse = None
        if hasattr(self, 'overlay_pipe') and os.path.exists(self.overlay_pipe):
            os.unlink(self.overlay_pipe)
        if hasattr(self, 'tmp_overlay_dir') and os.path.exists(self.tmp_overlay_dir):
            shutil.rmtree(self.tmp_overlay_dir)

def get_local_ipaddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ipaddress = (s.getsockname()[0])
    s.close()
    return ipaddress


class StreamSynthesisConst(object):
    SERVER_PORT_NUMBER = 8022
    VERSION = 0.1


class StreamSynthesisServer(SocketServer.TCPServer):
    def __init__(self, args):
        settings, args = StreamSynthesisServer.process_command_line(args)
        self.dbconn = DBConnector()
        self.basevm_list = self.check_basevm()

        StreamSynthesisConst.LOCAL_IPADDRESS = "0.0.0.0"
        server_address = (StreamSynthesisConst.LOCAL_IPADDRESS, StreamSynthesisConst.SERVER_PORT_NUMBER)

        self.allow_reuse_address = True
        try:
            SocketServer.TCPServer.__init__(self, server_address, StreamSynthesisHandler)
        except socket.error as e:
            sys.stderr.write(str(e))
            sys.stderr.write("Check IP/Port : %s\n" % (str(server_address)))
            sys.exit(1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        LOG.info("* Server configuration")
        LOG.info(" - Open TCP Server at %s" % (str(server_address)))
        LOG.info(" - Disable Nagle(No TCP delay)  : %s" \
                % str(self.socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)))
        LOG.info("-"*50)

    def handle_error(self, request, client_address):
        #SocketServer.TCPServer.handle_error(self, request, client_address)
        #sys.stderr.write("handling error from client %s\n" % (str(client_address)))
        pass

    def terminate(self):
        # close all thread
        if self.socket != -1:
            self.socket.close()

        global session_resources
        for (session_id, resource) in session_resources.iteritems():
            try:
                resource.deallocate()
                msg = "Deallocate resources for Session: %s" % str(session_id)
                LOG.info(msg)
            except Exception as e:
                msg = "Failed to deallocate resources for Session : %s" % str(session_id)
                LOG.warning(msg)
        LOG.info("[TERMINATE] Finish synthesis server connection")


    @staticmethod
    def process_command_line(argv):
        global operation_mode
        VERSION = 'VM Stream Server: %s' % StreamSynthesisConst.VERSION
        parser = OptionParser(usage="usage: %prog " + " [option]",
                version=VERSION)
        settings, args = parser.parse_args(argv)
        return settings, args

    def check_basevm(self):
        basevm_list = self.dbconn.list_item(BaseVM)
        ret_list = list()
        LOG.info("-"*50)
        LOG.info("* Base VM Configuration")
        for index, item in enumerate(basevm_list):
            # check file location
            (base_diskmeta, base_mempath, base_memmeta) = \
                    Cloudlet_Const.get_basepath(item.disk_path)
            if not os.path.exists(item.disk_path):
                LOG.warning("disk image (%s) is not exist" % (item.disk_path))
                continue
            if not os.path.exists(base_mempath):
                LOG.warning("memory snapshot (%s) is not exist" % (base_mempath))
                continue

            # add to list
            ret_list.append(item)
            LOG.info(" %d : %s (Disk %d MB, Memory %d MB)" % \
                    (index, item.disk_path, os.path.getsize(item.disk_path)/1024/1024, \
                    os.path.getsize(base_mempath)/1024/1024))
        LOG.info("-"*50)

        if len(ret_list) == 0:
            LOG.error("[Error] NO valid Base VM")
            sys.exit(2)
        return ret_list


if __name__ == "__main__":
    server = StreamSynthesisServer(sys.argv[1:])
    try:
        server.serve_forever()
    except Exception as e:
        #sys.stderr.write(str(e))
        server.terminate()
        sys.exit(1)
    except KeyboardInterrupt as e:
        sys.stdout.write("Exit by user\n")
        server.terminate()
        sys.exit(1)
    else:
        server.terminate()
        sys.exit(0)


