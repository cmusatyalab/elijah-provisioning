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
import functools
import traceback
import sys
import time
import struct
import Queue
import SocketServer
import socket
import subprocess
import collections
import tempfile
import multiprocessing
import threading
from hashlib import sha256

import delta
from server import NetworkUtil
from synthesis_protocol import Protocol as Protocol
from synthesis import run_fuse
from synthesis import SynthesizedVM
from synthesis import connect_vnc
from handoff import HandoffDataRecv

#import synthesis as synthesis
#from package import VMOverlayPackage
from db.api import DBConnector
from db.table_def import BaseVM
from configuration import Const as Cloudlet_Const
from compression import DecompProc
from pprint import pformat
import log as logging

import mmap
import tool
from delta import DeltaItem


LOG = logging.getLogger(__name__)
session_resources = dict()   # dict[session_id] = obj(SessionResource)


def wrap_process_fault(function):
    """Wraps a method to catch exceptions related to instances.
    This decorator wraps a method to catch any exceptions and
    terminate the request gracefully.
    """
    @functools.wraps(function)
    def decorated_function(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)
        except Exception, e:
            if hasattr(self, 'exception_handler'):
                self.exception_handler()
            kwargs.update(dict(zip(function.func_code.co_varnames[2:], args)))
            LOG.error("failed with : %s" % str(kwargs))

    return decorated_function


def try_except(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception, e:
            et, ei, tb = sys.exc_info()
            raise MyError, MyError(e), tb
    return wrapped


class StreamSynthesisError(Exception):
    pass


class AckThread(threading.Thread):
    def __init__(self, request):
        self.request = request
        self.ack_queue = Queue.Queue()
        threading.Thread.__init__(self, target=self.start_sending_ack)

    def start_sending_ack(self):
        while True:
            data = self.ack_queue.get()
            bytes_recved= self.ack_queue.get()
            # send ack
            ack_data = struct.pack("!Q", bytes_recved)
            self.request.sendall(ack_data)

    def signal_ack(self):
        self.ack_queue.put(bytes_recved)


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
        self.recovered_hash_dict = dict()
        self.live_migration_iteration_dict = dict()

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
        delta_counter = collections.Counter()
        delta_times = collections.Counter()
        unresolved_deltaitem_list = []
        while True:
            recv_data = self.decomp_delta_queue.get()
            if recv_data == Cloudlet_Const.QUEUE_SUCCESS_MESSAGE:
                break

            overlay_chunk_ids = list()
            # recv_data is a single blob so that it contains whole DeltaItem
            LOG.debug("%f\trecover one blob" % (time.time()))

            delta_item_list = RecoverDeltaProc.from_buffer(recv_data,delta_counter,delta_times)
            for delta_item in delta_item_list:
                ret = self.recover_item(delta_item,delta_counter,delta_times)
                if ret == None:
                    # cannot find self reference point due to the parallel
                    # compression. Save this and do it later
                    unresolved_deltaitem_list.append(delta_item)
                    continue
                self.process_deltaitem(delta_item, delta_counter,delta_times)
                count += 1
            start_time = time.time()
            self.recover_mem_fd.flush()
            self.recover_disk_fd.flush()
            delta_times['flush'] += (time.time() - start_time)
            #self.fuse_info_queue.put(overlay_chunk_ids)

        LOG.info("[Delta] Handle dangling DeltaItem (%d)" % len(unresolved_deltaitem_list))
        overlay_chunk_ids = list()
        for delta_item in unresolved_deltaitem_list:
            ret = self.recover_item(delta_item,delta_counter,delta_times)
            if ret == None:
                msg = "Cannot find self reference: type(%ld), offset(%ld), index(%ld)" % \
                        (delta_item.delta_type, delta_item.offset, delta_item.index)
                raise StreamSynthesisError(msg)
            self.process_deltaitem(delta_item, delta_counter,delta_times)
            count += 1

        self.recover_mem_fd.close()
        self.recover_mem_fd = None
        self.recover_disk_fd.close()
        self.recover_disk_fd = None
        #self.fuse_info_queue.put(overlay_chunk_ids)
        #self.fuse_info_queue.put(Cloudlet_Const.QUEUE_SUCCESS_MESSAGE)
        time_end = time.time()
        LOG.debug("Delta metrics:")
        LOG.debug("="*50)
        LOG.debug(delta_counter)
        LOG.debug(delta_times)
        LOG.debug(sum(delta_times.values()))
        LOG.info("[time] Delta delta %ld chunks, (%s~%s): %s" % \
                (count, time_start, time_end, (time_end-time_start)))
        LOG.info("Finish VM handoff")

    def recover_item(self, delta_item, delta_counter, delta_times):
        if type(delta_item) != DeltaItem:
            raise StreamSynthesisError("Need list of DeltaItem")

        delta_counter[delta_item.ref_id] += 1
        start_time = time.time()
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
        elif delta_item.ref_id == DeltaItem.REF_SELF_HASH:
            ref_hashvalue = delta_item.data
            self_ref_delta_item = self.recovered_hash_dict.get(ref_hashvalue, None)
            if self_ref_delta_item == None:
                return None
            recover_data = self_ref_delta_item.data
            delta_item.hash_value = ref_hashvalue
        elif delta_item.ref_id == DeltaItem.REF_XDELTA:
            patch_data = delta_item.data
            patch_original_size = delta_item.offset_len
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
            elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
            else:
                raise StreamSynthesisError("Delta type should be either disk or memory")
            recover_data = tool.merge_data(base_data, patch_data, len(base_data)*5)
        elif delta_item.ref_id == DeltaItem.REF_BSDIFF:
            patch_data = delta_item.data
            patch_original_size = delta_item.offset_len
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
            elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
            else:
                raise DeltaError("Delta type should be either disk or memory")
            recover_data = tool.merge_data_bsdiff(base_data, patch_data)
        elif delta_item.ref_id == DeltaItem.REF_XOR:
            patch_data = delta_item.data
            patch_original_size = delta_item.offset_len
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
            elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
            else:
                raise DeltaError("Delta type should be either disk or memory")
            recover_data = tool.cython_xor(base_data, patch_data)
        else:
            raise StreamSynthesisError("Cannot recover: invalid referce id %d" % delta_item.ref_id)

        if len(recover_data) != delta_item.offset_len:
            msg = "Error, Recovered Size Error: %d, %d, ref_id: %s, data_len: %ld, offset: %ld, offset_len: %ld" % \
                    (delta_item.delta_type, len(recover_data), delta_item.ref_id, \
                    delta_item.data_len, delta_item.offset, delta_item.offset_len)
            print msg
            raise StreamSynthesisError(msg)
        delta_times[delta_item.ref_id] += (time.time() - start_time)
        # recover
        delta_item.ref_id = DeltaItem.REF_RAW
        delta_item.data = recover_data
        if delta_item.hash_value == None or len(delta_item.hash_value) == 0:
            delta_counter['sha'] += 1
            start_time = time.time()
            delta_item.hash_value = sha256(recover_data).digest()
            delta_times['sha'] += (time.time() - start_time)

        return delta_item

    @staticmethod
    def from_buffer(data, delta_counter, delta_times):
        #import yappi
        #yappi.start()
        offset = 0
        deltaitem_list = list()
        while True:
            start_time = time.time()
            new_item, offset = RecoverDeltaProc.unpack_stream(data, offset=offset)
            if offset is None:
                break
            delta_times['unpack'] += (time.time() - start_time)
            deltaitem_list.append(new_item)
        #yappi.get_func_stats().print_all()
        return deltaitem_list

    @staticmethod
    def unpack_stream(stream, with_hashvalue=False, offset=0):
        if len(stream) <= offset:
            return None, None

        (ram_offset, offset_len, ref_info) = struct.unpack_from("!QHc", stream, offset)
        offset += struct.calcsize("!QHc")

        ref_id, delta_type = divmod(ord(ref_info), 16)

        data = ''
        data_len = 0
        live_seq = None

        if ref_id == DeltaItem.REF_RAW or \
                ref_id == DeltaItem.REF_XDELTA or \
                ref_id == DeltaItem.REF_XOR or \
                ref_id == DeltaItem.REF_BSDIFF:
            data_len = struct.unpack_from("!Q", stream, offset)[0]
            offset += struct.calcsize("!Q")
            data = stream[offset:offset+data_len]
            offset += data_len

        elif ref_id == DeltaItem.REF_SELF or \
                ref_id == DeltaItem.REF_BASE_DISK or \
                ref_id == DeltaItem.REF_BASE_MEM:
            data = struct.unpack_from("!Q", stream, offset)[0]
            offset += struct.calcsize("!Q")

        elif ref_id == DeltaItem.REF_SELF_HASH:
            #print "unpacking ref_self_hash"
            data = struct.unpack_from("!32s", stream, offset)[0]
            offset += struct.calcsize("!32s")

        if delta_type == DeltaItem.DELTA_DISK_LIVE or\
                delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            live_seq = struct.unpack_from("!H", stream, offset)[0]
            offset += struct.calcsize("!H")

        # hash_value typically does not exist when recovered becuase we don't need it
        if with_hashvalue:
            # hash_value is only needed for residue case
            hash_value = struct.unpack_from("!32s", stream, offset)[0]
            offset += struct.calcsize("!32s")
            item = DeltaItem(delta_type, ram_offset, offset_len, hash_value, ref_id, data_len, live_seq=live_seq)
        else:
            item = DeltaItem(delta_type, ram_offset, offset_len, None, ref_id, data_len, data, live_seq=live_seq)

        return item, offset

    def process_deltaitem(self, delta_item, delta_counter, delta_times):
        if len(delta_item.data) != delta_item.offset_len:
            msg = "recovered size is not same as page size, %ld != %ld" % \
                    (len(delta_item.data), delta_item.offset_len)
            raise StreamSynthesisError(msg)
        start_time = time.time()
        # save it to dictionary to find self_reference easily
        self.recovered_delta_dict[delta_item.index] = delta_item
        self.recovered_hash_dict[delta_item.hash_value] = delta_item
        delta_times['dict'] += (time.time() - start_time)
        # do nothing if the latest memory or disk are already process
        prev_iter_item = self.live_migration_iteration_dict.get(delta_item.index)
        if (prev_iter_item is not None):
            prev_seq = getattr(prev_iter_item, 'live_seq', 0)
            item_seq = getattr(delta_item, 'live_seq', 0)
            if prev_seq > item_seq:
                msg = "Latest version is already synthesized at %d (%d)" % (delta_item.offset, delta_item.delta_type)
                LOG.debug(msg)
                return

        # write to output file
        start_time = time.time()
        overlay_chunk_id = long(delta_item.offset/self.chunk_size)
        if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            self.recover_mem_fd.seek(delta_item.offset)
            self.recover_mem_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (RecoverDeltaProc.FUSE_INDEX_MEMORY, overlay_chunk_id))
        elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
            delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            self.recover_disk_fd.seek(delta_item.offset)
            self.recover_disk_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (RecoverDeltaProc.FUSE_INDEX_DISK, overlay_chunk_id))
        delta_times['seekwrite'] += (time.time() - start_time)

        # update the latest item for each memory page or disk block
        self.live_migration_iteration_dict[delta_item.index] = delta_item


    def finish(self):
        self.recovered_delta_dict.clear()
        self.recovered_delta_dict = None
        self.recovered_hash_dict.clear()
        self.recovered_hash_dict = None
        self.live_migration_iteration_dict.clear()
        self.live_migration_iteration_dict = None
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

    def _recv_all(self, recv_size, ack_size=sys.maxint):
        prev_ack_sent_size = 0
        data = ''
        while len(data) < recv_size:
            tmp_data = self.request.recv(recv_size-len(data))
            if tmp_data == None:
                raise StreamSynthesisError("Cannot recv data at %s" % str(self))
            if len(tmp_data) == 0:
                raise StreamSynthesisError("Recv 0 data at %s" % str(self))
            data += tmp_data

            # to send ack for every PERIODIC_ACK_BYTES bytes
            cur_recv_size = len(data)
            data_diff = cur_recv_size-prev_ack_sent_size
            if  data_diff > ack_size:
                ack_data = struct.pack("!Q", data_diff)
                self.request.sendall(ack_data)
                if (cur_recv_size-prev_ack_sent_size) >= ack_size*2:
                    #LOG.debug("we missed to send acks")
                    pass
                prev_ack_sent_size = cur_recv_size
        return data

    def _check_validity(self, message):
        header_info = None
        requested_base = None

        synthesis_option = message.get(Protocol.KEY_SYNTHESIS_OPTION, None)
        base_hashvalue = message.get(Cloudlet_Const.META_BASE_VM_SHA256, None)

        # check base VM
        for each_basevm in self.server.basevm_list:
            if base_hashvalue == each_basevm['hash_value']:
                LOG.info("New client request %s VM" % (each_basevm['diskpath']))
                requested_base = each_basevm['diskpath']
        return [synthesis_option, requested_base]

    def handle(self):
        '''Handle request from the client
        Each request follows this format:

        | header size | header | blob header size | blob header | blob data  |
        |  (4 bytes)  | (var)  | (4 bytes)        | (var bytes) | (var bytes)|
        '''
        if self.server.handoff_data is not None:
            LOG.debug("VM synthesis using OpenStack")
        else:
            LOG.debug("VM synthesis as standalone")

        # variable
        self.total_recved_size_cur = 0
        self.total_recved_size_prev = 0

        # get header
        data = self._recv_all(4)
        if data == None or len(data) != 4:
            raise StreamSynthesisError("Failed to receive first byte of header")
        message_size = struct.unpack("!I", data)[0]
        msgpack_data = self._recv_all(message_size)
        metadata = NetworkUtil.decoding(msgpack_data)
        launch_disk_size = metadata[Cloudlet_Const.META_RESUME_VM_DISK_SIZE]
        launch_memory_size = metadata[Cloudlet_Const.META_RESUME_VM_MEMORY_SIZE]

        synthesis_option, base_diskpath = self._check_validity(metadata)
        if base_diskpath == None:
            raise StreamSynthesisError("No matching base VM")
        if self.server.handoff_data:
            base_diskpath, base_diskmeta, base_mempath, base_memmeta =\
                self.server.handoff_data.base_vm_paths
        else:
            (base_diskmeta, base_mempath, base_memmeta) = \
                    Cloudlet_Const.get_basepath(base_diskpath, check_exist=True)
        LOG.info("  - %s" % str(pformat(self.synthesis_option)))
        LOG.info("  - Base VM     : %s" % base_diskpath)

        # variables for FUSE
        temp_synthesis_dir = tempfile.mkdtemp(prefix="cloudlet-comp-")
        launch_disk = os.path.join(temp_synthesis_dir, "launch-disk")
        launch_mem = os.path.join(temp_synthesis_dir, "launch-mem")
        memory_chunk_all = set()
        disk_chunk_all = set()

        # start pipelining processes
        network_out_queue = multiprocessing.Queue()
        decomp_queue = multiprocessing.Queue()
        fuse_info_queue = multiprocessing.Queue()
        decomp_proc = DecompProc(network_out_queue, decomp_queue, num_proc=4)
        decomp_proc.start()
        LOG.info("Start Decompression process")
        delta_proc = RecoverDeltaProc(base_diskpath, base_mempath,
                                    decomp_queue,
                                    launch_mem,
                                    launch_disk,
                                    Cloudlet_Const.CHUNK_SIZE,
                                    fuse_info_queue)
        delta_proc.start()
        LOG.info("Start Synthesis process")

        # get each blob
        recv_blob_counter = 0
        while True:
            data = self._recv_all(4)
            if data == None or len(data) != 4:
                raise StreamSynthesisError("Failed to receive first byte of header")
                break
            blob_header_size = struct.unpack("!I", data)[0]
            blob_header_raw = self._recv_all(blob_header_size)
            blob_header = NetworkUtil.decoding(blob_header_raw)
            blob_size = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_SIZE)
            if blob_size == None:
                raise StreamSynthesisError("Failed to receive blob")
            if blob_size == 0:
                LOG.debug("%f\tend of stream" % (time.time()))
                break
            blob_comp_type = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_COMPRESSION)
            blob_disk_chunk = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_DISK_CHUNKS)
            blob_memory_chunk = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_MEMORY_CHUNKS)

            # send ack right before getting the blob
            ack_data = struct.pack("!Q", 0x01)
            self.request.send(ack_data)
            compressed_blob = self._recv_all(blob_size, ack_size=200*1024)
            # send ack right after getting the blob
            ack_data = struct.pack("!Q", 0x02)
            self.request.send(ack_data)

            network_out_queue.put((blob_comp_type, compressed_blob))
            memory_chunk_all.update(blob_memory_chunk)
            disk_chunk_all.update(blob_disk_chunk)
            LOG.debug("%f\treceive one blob" % (time.time()))
            recv_blob_counter += 1

        network_out_queue.put(Cloudlet_Const.QUEUE_SUCCESS_MESSAGE)
        delta_proc.join()
        LOG.debug("%f\tdeltaproc join" % (time.time()))

        # We told to FUSE that we have everything ready, so we need to wait
        # until delta_proc fininshes. we cannot start VM before delta_proc
        # finishes, because we don't know what will be modified in the future
        time_fuse_start = time.time()
        fuse = run_fuse(Cloudlet_Const.CLOUDLETFS_PATH, Cloudlet_Const.CHUNK_SIZE,
                base_diskpath, launch_disk_size, base_mempath, launch_memory_size,
                resumed_disk=launch_disk,  disk_chunks=disk_chunk_all,
                resumed_memory=launch_mem, memory_chunks=memory_chunk_all,
                valid_bit=1)
        time_fuse_end = time.time()
        memory_path = os.path.join(fuse.mountpoint, 'memory', 'image')

        if self.server.handoff_data:
            synthesized_vm = SynthesizedVM(
                launch_disk, launch_mem, fuse,
                disk_only=False, qemu_args=None,
                nova_xml=self.server.handoff_data.libvirt_xml,
                nova_conn=self.server.handoff_data._conn,
                nova_util=self.server.handoff_data._libvirt_utils
            )
        else:
            synthesized_vm = SynthesizedVM(launch_disk, launch_mem, fuse)

        synthesized_vm.start()
        synthesized_vm.join()

        # to be delete
        #libvirt_xml = synthesized_vm.new_xml_str
        #vmpaths = [base_diskpath, base_diskmeta, base_mempath, base_memmeta]
        #base_hashvalue = metadata.get(Cloudlet_Const.META_BASE_VM_SHA256, None)
        #ds = HandoffDataRecv()
        #ds.save_data(vmpaths, base_hashvalue, libvirt_xml, "qemu:///session")
        #ds.to_file("/home/stack/cloudlet/provisioning/handff_recv_data")

        # since libvirt does not return immediately after resuming VM, we
        # measure resume time directly from QEMU
        actual_resume_time = 0
        splited_log = open("/tmp/qemu_debug_messages", "r").read().split("\n")
        for line in splited_log:
            if line.startswith("INCOMING_FINISH"):
                actual_resume_time = float(line.split(" ")[-1])
        time_resume_end = time.time()
        LOG.info("[time] non-pipelined time %f (%f ~ %f ~ %f)" % (
            actual_resume_time-time_fuse_start,
            time_fuse_start,
            time_fuse_end,
            actual_resume_time,
        ))
        if self.server.handoff_data == None:
            # for a standalone version, terminate a VM for the next testing
            #connect_vnc(synthesized_vm.machine)
            LOG.debug("Finishing VM in 3 seconds")
            time.sleep(3)
            synthesized_vm.monitor.terminate()
            synthesized_vm.monitor.join()
            synthesized_vm.terminate()

        # send end message
        ack_data = struct.pack("!Qd", 0x10, actual_resume_time)
        LOG.info("send ack to client: %d" % len(ack_data))
        self.request.sendall(ack_data)
        LOG.info("finished")


    def terminate(self):
        # force terminate when something wrong in handling request
        # do not wait for joinining
        if hasattr(self, 'delta_proc') and self.delta_proc != None:
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
    def __init__(self, port_number=StreamSynthesisConst.SERVER_PORT_NUMBER,
                 timeout=None, handoff_datafile=None):
        self.port_number = port_number
        self.timeout = timeout
        self._handoff_datafile = handoff_datafile
        if self._handoff_datafile:
            self.handoff_data = self._load_handoff_data(self._handoff_datafile)
            self.basevm_list = self.check_basevm(
                self.handoff_data.base_vm_paths,
                self.handoff_data.basevm_sha256_hash
            )
        else:
            self.handoff_data = None
            self.basevm_list = self.check_basevm_from_db(DBConnector())

        server_address = ("0.0.0.0", self.port_number)
        self.allow_reuse_address = True
        try:
            SocketServer.TCPServer.__init__(self, server_address, StreamSynthesisHandler)
        except socket.error as e:
            sys.stderr.write(str(e))
            sys.stderr.write("Check IP/Port : %s\n" % (str(server_address)))
            sys.exit(1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        LOG.info("* Server configuration")
        LOG.info(" - Open TCP Server at %s" % (str(server_address)))
        LOG.info(" - Time out for waiting: %d" % (self.timeout))
        LOG.info(" - Disable Nagle(No TCP delay)  : %s" \
                % str(self.socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)))
        LOG.info("-"*50)

    def _load_handoff_data(self, filepath):
        handoff_data = HandoffDataRecv.from_file(filepath)
        if handoff_data == None:
            raise StreamSynthesisError("Invalid handoff recv data at %s" % filepath)
        LOG.info("Load handoff data file at %s" % filepath)
        return handoff_data

    def handle_error(self, request, client_address):
        SocketServer.TCPServer.handle_error(self, request, client_address)
        sys.stderr.write("handling error from client %s\n" % (str(client_address)))
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("%s" % str(e))

    def handle_timeout(self):
        sys.stderr.write("timeout error\n")

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

    def check_basevm(self, base_vm_paths, hash_value):
        ret_list = list()
        LOG.info("-"*50)
        LOG.info("* Base VM Configuration")
        # check file location
        (base_diskpath, base_diskmeta, base_mempath, base_memmeta) = base_vm_paths
        if not os.path.exists(base_diskpath):
            LOG.warning("base disk is not available at %s" % base_diskpath)
        if not os.path.exists(base_mempath):
            LOG.warning("base memory is not available at %s" % base_mempath)
        if not os.path.exists(base_diskmeta):
            LOG.warning("disk hashlist is not available at %s" % base_diskmeta)
        if not os.path.exists(base_memmeta):
            LOG.warning("memory hashlist is not available at %s" % base_memmeta)
        basevm_item = {'hash_value':hash_value, 'diskpath':base_diskpath}
        ret_list.append(basevm_item)

        LOG.info("  %s (Disk %d MB, Memory %d MB)" % \
                (base_diskpath, os.path.getsize(base_diskpath)/1024/1024, \
                os.path.getsize(base_mempath)/1024/1024))
        LOG.info("-"*50)
        return ret_list

    def check_basevm_from_db(self, dbconn):
        basevm_list = dbconn.list_item(BaseVM)
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
            basevm_item = {'hash_value':item.hash_value, 'diskpath':item.disk_path}
            ret_list.append(basevm_item)
            LOG.info(" %d : %s (Disk %d MB, Memory %d MB)" % \
                    (index, item.disk_path, os.path.getsize(item.disk_path)/1024/1024, \
                    os.path.getsize(base_mempath)/1024/1024))
        LOG.info("-"*50)

        if len(ret_list) == 0:
            LOG.error("[Error] NO valid Base VM")
            sys.exit(2)
        return ret_list
