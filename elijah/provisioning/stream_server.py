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
import signal
import collections
import tempfile
import multiprocessing
import Queue
import threading
from hashlib import sha256
import libvirt
import shutil
from tempfile import mkdtemp
from urlparse import urlsplit
import msgpack
import json

from server import NetworkUtil
from synthesis_protocol import Protocol as Protocol
from synthesis import run_fuse
from synthesis import SynthesizedVM
from synthesis import connect_vnc
from synthesis import increment_filename
import handoff
from elijah.provisioning.package import PackagingUtil

from db.api import DBConnector, update_op, log_op
from db.table_def import BaseVM
from db import table_def
from configuration import Options
from configuration import Const as Cloudlet_Const
from compression import DecompProc
from pprint import pformat
import logging
import logging.config
import random
import png
import mmap
import tool
from delta import DeltaItem

with open('/var/nephele/logging.json') as f:
    config_dict = json.load(f)
    logging.config.dictConfig(config_dict)

LOG = logging.getLogger(__name__)
session_resources = dict()   # dict[session_id] = obj(SessionResource)
HANDOFF_TEMP = '/tmp/.cloudlet-handoff'
HANDOFF_SIGNAL_RECEIVED = False

class StreamSynthesisError(Exception):
    pass

class RecoverDeltaProc(multiprocessing.Process):
    FUSE_INDEX_DISK = 1
    FUSE_INDEX_MEMORY = 2

    def __init__(self, base_disk, base_mem,
                 decomp_delta_queue, output_mem_path,
                 output_disk_path, chunk_size,
                 fuse_info_queue, analysis_queue):
        if base_disk is None and base_mem is None:
            raise StreamSynthesisError("Need either base_disk or base_memory")

        self.decomp_delta_queue = decomp_delta_queue
        self.output_mem_path = output_mem_path
        self.output_disk_path = output_disk_path
        self.fuse_info_queue = fuse_info_queue
        self.analysis_queue = analysis_queue
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
        self.recover_mem_fd = open(self.output_mem_path, "wb")
        self.recover_disk_fd = open(self.output_disk_path, "wb")
        delta_counter = collections.Counter()
        delta_times = collections.Counter()
        unresolved_deltaitem_list = []
        while True:
            recv_data = self.decomp_delta_queue.get()
            if recv_data == Cloudlet_Const.QUEUE_SUCCESS_MESSAGE:
                break

            overlay_chunk_ids = list()
            # recv_data is a single blob so that it contains whole DeltaItem

            unpack_start = time.time()
            delta_item_list = RecoverDeltaProc.from_buffer(recv_data,delta_counter,delta_times)
            self.analysis_queue.put("B,U,%5.3f" % (time.time() - unpack_start))
            for delta_item in delta_item_list:
                ret = self.recover_item(delta_item,delta_counter,delta_times)
                if ret is None:
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

        self.analysis_queue.put("Handling (%d) unresolved delta items." % len(unresolved_deltaitem_list))
        overlay_chunk_ids = list()
        for delta_item in unresolved_deltaitem_list:
            ret = self.recover_item(delta_item,delta_counter,delta_times)
            if ret is None:
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
        self.analysis_queue.put("="*50)
        self.analysis_queue.put("Delta metrics:")
        self.analysis_queue.put("="*50)
        self.analysis_queue.put("%r" % delta_counter)
        self.analysis_queue.put("%r" % delta_times)
        self.analysis_queue.put("Total captured time: %d" % (sum(delta_times.values())))
        self.analysis_queue.put("="*50)
        self.analysis_queue.put("Delta process recovered %ld chunks in %s seconds." % \
                (count,  (time_end-time_start)))

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
            if self_ref_delta_item is None:
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
                raise StreamSynthesisError("Delta type should be either disk or memory")
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
                raise StreamSynthesisError("Delta type should be either disk or memory")
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

        ref_id = delta_item.ref_id
        # recover
        delta_item.ref_id = DeltaItem.REF_RAW
        delta_item.data = recover_data
        if delta_item.delta_type == DeltaItem.DELTA_MEMORY or delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            self.analysis_queue.put("M,R(%d),%d" % (ref_id, delta_item.offset / 4096))
        elif delta_item.delta_type == DeltaItem.DELTA_DISK or delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            self.analysis_queue.put("D,R(%d),%d" % (ref_id, delta_item.offset / 4096))

        if delta_item.hash_value is None or len(delta_item.hash_value) == 0:
            delta_counter['sha'] += 1
            start_time = time.time()
            delta_item.hash_value = sha256(recover_data).digest()
            delta_times['sha'] += (time.time() - start_time)

        return delta_item

    @staticmethod
    def from_buffer(data, delta_counter, delta_times):
        offset = 0
        deltaitem_list = list()
        while True:
            start_time = time.time()
            new_item, offset = RecoverDeltaProc.unpack_stream(data, offset=offset)
            if offset is None:
                break
            delta_times['unpack'] += (time.time() - start_time)
            deltaitem_list.append(new_item)
        return deltaitem_list

    @staticmethod
    def unpack_stream(stream, with_hashvalue=False, offset=0):
        if len(stream) <= offset:
            return None, None

        (ram_offset, offset_len, ref_info) = struct.unpack_from("!QHc", stream, offset)
        offset += struct.calcsize("!QHc")

        ref_id = ord(ref_info) & 0xF0
        delta_type = ord(ref_info) & 0x0F

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
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or \
                                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                    msg = "M,P(%d),%d" % (delta_item.ref_id, delta_item.offset / 4096)
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or \
                                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                    msg = "D,P(%d),%d" % (delta_item.ref_id, delta_item.offset / 4096)
                self.analysis_queue.put(msg)
                return

        # write to output file
        start_time = time.time()
        if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            self.recover_mem_fd.seek(delta_item.offset)
            self.recover_mem_fd.write(delta_item.data)
            self.analysis_queue.put("M,A,%d" % (delta_item.offset / 4096))
        elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
            delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            self.recover_disk_fd.seek(delta_item.offset)
            self.recover_disk_fd.write(delta_item.data)
            self.analysis_queue.put("D,A,%d" % (delta_item.offset / 4096))
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

def chunk2mb(n):
    return (n * 4096) / float(1024*1024)

class HandoffAnalysisProc(multiprocessing.Process):
    def __init__(self, handoff_url, message_queue, disk_size, mem_size):
        self.url = handoff_url
        self.mq = message_queue
        self.viz_interval = 1 #1000 msecs
        self.disk_chunks = disk_size / 4096
        self.mem_chunks = mem_size / 4096
        self.time_start = self.last_update =  time.time()
        self.num_blobs = 0
        self.total_blob_size = 0
        self.disk_chunks_by_value = 0
        self.disk_chunks_by_ref = 0
        self.disk_chunks_zeroes = 0
        self.mem_chunks_by_value = 0
        self.mem_chunks_by_ref = 0
        self.mem_chunks_zeroes = 0
        self.disk_applied = 0
        self.mem_applied = 0
        self.iter = 0
        self.outfd = open(name='/var/tmp/cloudlet/handoff_from_%s_at_%d.log' % (self.url, self.time_start), mode = 'w')
        self.stats_path = '/var/www/html/heatmap/stats.txt'

        self.disk_path = '/var/www/html/heatmap/disk.png'
        self.disk_width, self.disk_height = self.layout(self.disk_chunks)
        self.disk_img_data = [[0 for _ in range(self.disk_width)] for _ in range(self.disk_height)]
        self.disk_writer = png.Writer(self.disk_width, self.disk_height, greyscale=True)

        self.mem_path = '/var/www/html/heatmap/mem.png'
        self.mem_width, self.mem_height = self.layout(self.mem_chunks)
        self.mem_img_data = [[0 for _ in range(self.mem_width)] for _ in range(self.mem_height)]
        self.mem_writer = png.Writer(self.mem_width, self.mem_height, greyscale=True)
        multiprocessing.Process.__init__(self, target=self.read_queue)

    def compute_factors(self,n):
        return sorted(set(reduce(list.__add__,
                          ([i, n / i] for i in range(1, int(n ** 0.5) + 1) if n % i == 0))))

    def layout(self,n):
        factors = self.compute_factors(n)
        width = factors[len(factors) / 2]
        height = factors[(len(factors) / 2) - 1]
        while (float(height) / float(width) < 0.5):
            n = n + 1
            factors = self.compute_factors(n)
            width = factors[len(factors) / 2]
            height = factors[(len(factors) / 2) - 1]
        return width, height

    def update_html_stats(self, handoff_complete=False):
        self.stats_file = open(name=self.stats_path, mode = 'w')
        self.stats_file.write('<div class="stats-col">')
        self.stats_file.write('Elapsed Time: %f seconds<br/>' % (time.time() - self.time_start))
        self.stats_file.write('Disk Size: %d MB<br/>' % ((self.disk_chunks * 4096) / (1024 * 1024)))
        self.stats_file.write('Memory Size: %d MB<br/>' % ((self.mem_chunks * 4096) / (1024 * 1024)))
        if (handoff_complete):
            self.stats_file.write('<span class="success">Handoff Complete!</span><br/>')
        self.stats_file.write('</div>')

        self.stats_file.write('<div class="stats-col">')
        self.stats_file.write('Iterations: %d <br/>' % self.iter)
        self.stats_file.write('Blobs Received (Total Size): %d (%d) <br/>' % (self.num_blobs, self.total_blob_size))
        self.stats_file.write('Disk Chunks Applied: %d <br/>' % self.disk_applied)
        self.stats_file.write('Memory Chunks Applied: %d <br/>' % self.mem_applied)
        self.stats_file.write('</div>')

        self.stats_file.flush()
        self.stats_file.close()



    def update_text_stats(self):
        outfd = open(name='/var/tmp/cloudlet/handoff.stats', mode = 'w')
        outfd.write('Iterations: %d\n' % self.iter)
        outfd.write('Elapsed Time: %f seconds\n' % (time.time() - self.time_start))
        outfd.write('VM Disk Size: %d MB\n' % (chunk2mb(self.disk_chunks)))
        outfd.write('VM Memory Size: %d MB\n' % (chunk2mb(self.mem_chunks)))
        outfd.write('\n\n')
        outfd.write('Blobs Received: %d\n' % self.num_blobs)
        outfd.write('Total MBytes Received over wire: %.2f\n' % (self.total_blob_size / float(1024*1024)))
        outfd.write('\n\n')
        outfd.write('VM STATE (MBytes)\n')
        outfd.write("{:<12}{:^8}{:<8}{:^4}{:<8}{:^4}{:<8}{:^4}{:<8}\n".format('', '', 'DIRTIED', '','BY VALUE', '', 'BY REF', '','ZEROS'))
        outfd.write("{:<12}{:^8}{:<8.2f}{:^4}{:<8.2f}{:^4}{:<8.2f}{:^4}{:<8.2f}\n".format('MEMORY', '', chunk2mb(self.mem_applied), '', chunk2mb(self.mem_chunks_by_value), '', chunk2mb(self.mem_chunks_by_ref), '', chunk2mb(self.mem_chunks_zeroes)))
        outfd.write("{:<12}{:^8}{:<8.2f}{:^4}{:<8.2f}{:^4}{:<8.2f}{:^4}{:<8.2f}\n".format('DISK', '', chunk2mb(self.disk_applied), '', chunk2mb(self.disk_chunks_by_value), '', chunk2mb(self.disk_chunks_by_ref), '', chunk2mb(self.disk_chunks_zeroes)))
        outfd.flush()
        outfd.close()

    def update_imgs(self):
        #DISK
        disk_jpg = open(self.disk_path, 'wb')
        self.disk_writer.write(disk_jpg, self.disk_img_data)
        disk_jpg.close()

        #MEMORY
        mem_jpg = open(self.mem_path, 'wb')
        self.mem_writer.write(mem_jpg, self.mem_img_data)
        mem_jpg.close()

    def read_queue(self):
        while True:
            self._running = True
            if((time.time() - self.last_update) > self.viz_interval):
                self.update_html_stats()
                self.update_text_stats()
                self.update_imgs()
                self.outfd.write('%f ' % (time.time() - self.time_start))
                self.outfd.write("update_imgs")
                self.outfd.write("\n")
                self.last_update = time.time()

            try:
                message = self.mq.get(False)
                if message == "!E_O_Q!":
                    self.outfd.flush() #flush remaining buffer
                    self.outfd.close()
                    self.update_html_stats(handoff_complete=True)
                    self.update_text_stats()
                    os.rename(self.disk_path, '/var/tmp/cloudlet/disk_from_%s_at_%d.png' % (self.url, self.time_start))
                    os.rename(self.mem_path, '/var/tmp/cloudlet/mem_from_%s_at_%d.png' % (self.url, self.time_start))
                    #os.remove(self.stats_path)
                    break
                elif message.startswith("M,A,"):
                    #parse message to find index in third token
                    # update self.mem_img_data
                    tokens = message.split(',')
                    if len(tokens) == 3 and tokens[2].isdigit():
                        id = int(tokens[2])
                        heat = self.mem_img_data[id / self.mem_width][id % self.mem_width]
                        self.mem_img_data[id / self.mem_width][id % self.mem_width] = 255 if ((heat+64) >= 255) else heat + 64
                        self.mem_applied += 1
                elif message.startswith("D,A,"):
                    #parse message to find index in third token
                    #update self.disk_img_data
                    tokens = message.split(',')
                    if len(tokens) == 3 and tokens[2].isdigit():
                        id = int(tokens[2])
                        heat = self.disk_img_data[id / self.disk_width][id % self.disk_width]
                        self.disk_img_data[id / self.disk_width][id % self.disk_width] = 255 if ((heat+64) >= 255) else heat + 64
                        self.disk_applied += 1
                elif message.startswith("D,R"):
                    lindex = message.find("(")
                    rindex = message.find(")")
                    if(int(message[lindex+1:rindex]) in [16,32,112,144]):
                        self.disk_chunks_by_value += 1
                    elif (int(message[lindex+1:rindex]) in [96]):
                        self.disk_chunks_zeroes += 1
                    else:
                        self.disk_chunks_by_ref += 1
                elif message.startswith("M,R"):
                    lindex = message.find("(")
                    rindex = message.find(")")
                    if(int(message[lindex+1:rindex]) in [16,32,112,144]):
                        self.mem_chunks_by_value += 1
                    elif (int(message[lindex+1:rindex]) in [96]):
                        self.mem_chunks_zeroes += 1
                    else:
                        self.mem_chunks_by_ref += 1
                elif message.startswith("B,R,"):
                    self.num_blobs += 1
                    tokens = message.split(',')
                    if len(tokens) == 3 and tokens[2].isdigit():
                        self.total_blob_size += int(tokens[2])
                elif message.startswith("iter,"):
                    tokens = message.split(',')
                    if len(tokens) == 2 and tokens[1].isdigit():
                        self.iter = int(tokens[1])

                self.outfd.write('%f ' % (time.time()-self.time_start))
                self.outfd.write(message)
                self.outfd.write("\n")
            except EOFError:
                break
            except Queue.Empty:
                pass
            except IndexError:
                LOG.error("%s" % str(message))
                pass

    def terminate(self):
        self.outfd.close()

class StreamSynthesisHandler(SocketServer.StreamRequestHandler):
    synthesis_option = {
            Protocol.SYNTHESIS_OPTION_DISPLAY_VNC: False,
            Protocol.SYNTHESIS_OPTION_EARLY_START: False,
            Protocol.SYNTHESIS_OPTION_SHOW_STATISTICS: False
            }

    def ret_fail(self, message):
        LOG.error("%s" % str(message))
        message = NetworkUtil.encoding({
            Protocol.KEY_COMMAND: Protocol.MESSAGE_COMMAND_FAILED,
            Protocol.KEY_FAILED_REASON: message
            })
        message_size = struct.pack("!I", len(message))
        self.request.send(message_size)
        self.wfile.write(message)

    def ret_success(self, req_command, payload=None):
        send_message = {
            Protocol.KEY_COMMAND: Protocol.MESSAGE_COMMAND_SUCCESS,
            Protocol.KEY_REQUESTED_COMMAND: req_command,
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
            Protocol.KEY_COMMAND: Protocol.MESSAGE_COMMAND_SYNTHESIS_DONE,
            })
        LOG.info("SUCCESS to launch VM")
        try:
            message_size = struct.pack("!I", len(message))
            self.request.send(message_size)
            self.wfile.write(message)
        except socket.error as e:
            pass

    def _recv_all(self, recv_size, ack_size=1024*1024):
        prev_ack_sent_size = 0
        data = ''
        while len(data) < recv_size:
            tmp_data = self.request.recv(recv_size-len(data))
            if tmp_data is None:
                raise StreamSynthesisError("Cannot recv data at %s" % str(self))
            if len(tmp_data) == 0:
                raise StreamSynthesisError("Recv 0 data at %s" % str(self))
            data += tmp_data

            # to send ack for every PERIODIC_ACK_BYTES bytes
            cur_recv_size = len(data)
            data_diff = cur_recv_size-prev_ack_sent_size
            if data_diff > ack_size or cur_recv_size >= recv_size:
                ack_data = struct.pack("!Q", data_diff)
                self.request.sendall(ack_data)
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

    def handlesig(self,signum, frame):
        global HANDOFF_SIGNAL_RECEIVED
        LOG.info("Received signal(%d) to start handoff..." % signum)
        HANDOFF_SIGNAL_RECEIVED = True

    def handle(self):
        '''Handle request from the client
        Each request follows this format:

        | header size | header | blob header size | blob header | blob data  |
        |  (4 bytes)  | (var)  | (4 bytes)        | (var bytes) | (var bytes)|
        '''

        LOG.info("Incoming request from %s", self.client_address)
        global HANDOFF_SIGNAL_RECEIVED
        # variable
        self.total_recved_size_cur = 0
        self.total_recved_size_prev = 0

        # get header
        data = self._recv_all(4)
        if data is None or len(data) != 4:
            LOG.error("Incorrect header size received from client!")
            raise StreamSynthesisError("Failed to receive first byte of header")
        message_size = struct.unpack("!I", data)[0]
        msgpack_data = self._recv_all(message_size)
        metadata = NetworkUtil.decoding(msgpack_data)
        launch_disk_size = metadata[Cloudlet_Const.META_RESUME_VM_DISK_SIZE]
        launch_memory_size = metadata[Cloudlet_Const.META_RESUME_VM_MEMORY_SIZE]
        title = metadata[Cloudlet_Const.META_VM_TITLE]
        fwd_ports = metadata[Cloudlet_Const.META_FWD_PORTS]
        base_hash = metadata[Cloudlet_Const.META_BASE_VM_SHA256]

        analysis_mq = multiprocessing.Queue()
        analysis_proc = HandoffAnalysisProc(handoff_url=self.client_address[0],message_queue=analysis_mq, disk_size=launch_disk_size, mem_size=launch_memory_size)
        analysis_proc.start()

        analysis_mq.put("=" * 50)
        analysis_mq.put("Adaptive VM Handoff Initiated")
        analysis_mq.put("Client Connection - %s:%d" % (self.client_address[0], self.client_address[1])) #client_address is a tuple (ip, port)

        if self.server.handoff_data is not None:
            analysis_mq.put("Handoff via OpenStack")
            via_openstack = True
        else:
            analysis_mq.put("Handoff via cloudlet CLI")
            via_openstack = False

        synthesis_option, base_diskpath = self._check_validity(metadata)
        if base_diskpath is None:
            raise StreamSynthesisError("No matching base VM")
        if via_openstack:
            base_diskpath, base_mempath, base_diskmeta, base_memmeta = self.server.handoff_data.base_vm_paths
        else:
            (base_diskmeta, base_mempath, base_memmeta) = Cloudlet_Const.get_basepath(base_diskpath, check_exist=True)
        analysis_mq.put("Synthesis Options %s" % str(pformat(self.synthesis_option)))
        analysis_mq.put("Base VM Path: %s" % base_diskpath)
        analysis_mq.put("Image Disk Size: %d" % launch_disk_size)
        analysis_mq.put("Image Memory Size: %d" % launch_memory_size)
        analysis_mq.put("=" * 50)
        # variables for FUSE
        if via_openstack:
            launch_disk = self.server.handoff_data.launch_diskpath
            launch_mem = self.server.handoff_data.launch_memorypath
        else:
            temp_synthesis_dir = tempfile.mkdtemp(prefix="cloudlet-comp-")
            launch_disk = os.path.join(temp_synthesis_dir, "launch-disk")
            launch_mem = os.path.join(temp_synthesis_dir, "launch-mem")
        memory_chunk_all = set()
        disk_chunk_all = set()

        # start pipelining processes
        network_out_queue = multiprocessing.Queue()
        decomp_queue = multiprocessing.Queue()
        fuse_info_queue = multiprocessing.Queue()
        decomp_proc = DecompProc(network_out_queue, decomp_queue, num_proc=4, analysis_queue=analysis_mq)
        decomp_proc.start()
        analysis_mq.put("Starting (%d) decompression processes..." % (decomp_proc.num_proc))
        delta_proc = RecoverDeltaProc(base_diskpath, base_mempath,
                                    decomp_queue,
                                    launch_mem,
                                    launch_disk,
                                    Cloudlet_Const.CHUNK_SIZE,
                                    fuse_info_queue,
                                    analysis_mq)
        delta_proc.start()
        analysis_mq.put("Starting delta recovery process...")

        # get each blob
        recv_blob_counter = 0
        while True:
            data = self._recv_all(4)
            if data is None or len(data) != 4:
                raise StreamSynthesisError("Failed to receive first byte of header")

            blob_header_size = struct.unpack("!I", data)[0]
            blob_header_raw = self._recv_all(blob_header_size)
            blob_header = NetworkUtil.decoding(blob_header_raw)
            blob_size = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_SIZE)
            if blob_size is None:
                raise StreamSynthesisError("Failed to receive blob")
            if blob_size == 0:
                analysis_mq.put("End of stream received from client at %f)" % (time.time()))
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
            #TODO: remove the interweaving of the valid bit here
            #TODO: and change the code path in cloudlet_driver.py so that
            #TODO: it uses the chunk sets in favor of the tuples
            if via_openstack:
                memory_chunk_set = set(["%ld:1" % item for item in blob_memory_chunk])
                disk_chunk_set = set(["%ld:1" % item for item in blob_disk_chunk])
                memory_chunk_all.update(memory_chunk_set)
                disk_chunk_all.update(disk_chunk_set)
            else:
                memory_chunk_all.update(blob_memory_chunk)
                disk_chunk_all.update(blob_disk_chunk)
            recv_blob_counter += 1
            analysis_mq.put("B,R,%d" % (blob_size))
            data = self._recv_all(4)
            iter = struct.unpack("!I", data)[0]
            analysis_mq.put("iter,%d" % (iter))

        network_out_queue.put(Cloudlet_Const.QUEUE_SUCCESS_MESSAGE)
        delta_proc.join()
        LOG.debug("%f\tdeltaproc join" % (time.time()))


        analysis_mq.put("Adaptive VM Handoff Complete!")
        analysis_mq.put("=" * 50)
        analysis_mq.put("!E_O_Q!")
        analysis_proc.join()

        if via_openstack:
            ack_data = struct.pack("!Qd", 0x10, time.time())
            LOG.info("send ack to client: %d" % len(ack_data))
            self.request.sendall(ack_data)

            disk_overlay_map = ','.join(disk_chunk_all)
            memory_overlay_map = ','.join(memory_chunk_all)
            # NOTE: fuse and synthesis take place in cloudlet_driver.py when launched from openstack but
            #this data must be written to stdout so the pipe connected to cloudlet_driver.py can finish the handoff
            #TODO: instead of sending this stdout buffer over the pipe to cloudlet_driver.py, we should probably
            #TODO: move to multiprocessing.Pipe or Queue to avoid issues with other items being dumped to stdout
            #TODO: and causing problems with this data being sent back; i.e. anything written via LOG
            #TODO: after this will end up in stdout because the logger has a StreamHandler configured to use stdout
            sys.stdout.write("openstack\t%s\t%s\t%s\t%s" % (launch_disk_size, launch_memory_size, disk_overlay_map, memory_overlay_map))

        else:
            # save the instance info to DB
            dbconn = DBConnector()
            new = table_def.Instances(title, os.getpid())
            dbconn.add_item(new)

            # We told to FUSE that we have everything ready, so we need to wait
            # until delta_proc finishes. we cannot start VM before delta_proc
            # finishes, because we don't know what will be modified in the future
            time_fuse_start = time.time()
            fuse = run_fuse(Cloudlet_Const.CLOUDLETFS_PATH, Cloudlet_Const.CHUNK_SIZE,
                            base_diskpath, launch_disk_size, base_mempath, launch_memory_size,
                            resumed_disk=launch_disk, disk_chunks=disk_chunk_all,
                            resumed_memory=launch_mem, memory_chunks=memory_chunk_all,
                            valid_bit=1)
            time_fuse_end = time.time()

            synthesized_vm = SynthesizedVM(launch_disk, launch_mem, fuse, title=title)

            synthesized_vm.start()
            synthesized_vm.join()

            # since libvirt does not return immediately after resuming VM, we
            # measure resume time directly from QEMU
            actual_resume_time = 0
            splited_log = open("/tmp/qemu_debug_messages", "r").read().split("\n")
            for line in splited_log:
                if line.startswith("INCOMING_FINISH"):
                    actual_resume_time = float(line.split(" ")[-1])

            LOG.info("[time] non-pipelined time %f (%f ~ %f ~ %f)" % (
                actual_resume_time-time_fuse_start,
                time_fuse_start,
                time_fuse_end,
                actual_resume_time,
            ))

            ack_data = struct.pack("!Qd", 0x10, actual_resume_time)
            LOG.info("send ack to client: %d" % len(ack_data))
            self.request.sendall(ack_data)

            preload_thread = handoff.PreloadResidueData(
                base_diskmeta, base_memmeta)
            preload_thread.daemon = True
            preload_thread.start()
            save_snapshot = False
            signal.signal(signal.SIGUSR1, self.handlesig)
            while True:
                state, _ = synthesized_vm.machine.state()
                if state == libvirt.VIR_DOMAIN_PAUSED:
                    #make a new snapshot and store it
                    handoff_url = 'file:///root/%s' % (increment_filename(title)) 
                    save_snapshot = True
                    print 'VM entered paused state. Generating snapshot of disk and memory...'
                    break
                elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
                    #disambiguate between reboot and shutoff
                    time.sleep(1)
                    try:
                        if synthesized_vm.machine.isActive():
                            state, _ = synthesized_vm.machine.state()
                            if state == libvirt.VIR_DOMAIN_RUNNING:
                                continue
                            else:
                                print "VM has been powered off. Tearing down FUSE..."
                                synthesized_vm.terminate()
                        else:
                            print "VM is no longer running. Tearing down FUSE..."
                            synthesized_vm.terminate()
                    except libvirt.libvirtError as e:
                        synthesized_vm.terminate()
                    finally:
                        dbconn = DBConnector()
                        list = dbconn.list_item(table_def.Instances)
                        for item in list:
                            if title == item.title:
                                dbconn.del_item(item)
                                break
                        return
                elif HANDOFF_SIGNAL_RECEIVED == True:
                    #read destination from file
                    fdest = open(HANDOFF_TEMP, "rb")
                    meta = msgpack.unpackb(fdest.read())
                    fdest.close()
                    #validate that the meta data is really for us
                    if meta['pid'] == os.getpid():
                        handoff_url = meta['url']
                        print 'Handoff initiated for %s to the following destination: %s' % (meta['title'], meta['url'])
                        op_id = log_op(op=Cloudlet_Const.OP_HANDOFF,notes="Title: %s, PID: %d, Dest: %s" % (meta['title'], meta['pid'], handoff_url))
                        HANDOFF_SIGNAL_RECEIVED = False
                        os.remove(HANDOFF_TEMP)
                        break
                    else:
                        print 'PID in %s does not match getpid!' % HANDOFF_TEMP
        

            options = Options()
            options.TRIM_SUPPORT = True
            options.FREE_SUPPORT = True
            options.DISK_ONLY = False
            preload_thread.join()
            (base_diskmeta, base_mem, base_memmeta) = \
                Cloudlet_Const.get_basepath(base_diskpath, check_exist=False)
            base_vm_paths = [base_diskpath, base_mem, base_diskmeta, base_memmeta]
            # prepare data structure for VM handoff
            residue_zipfile = None
            dest_handoff_url = handoff_url
            parsed_handoff_url = urlsplit(handoff_url)
            if parsed_handoff_url.scheme == "file":
                dest_path = parsed_handoff_url.path
                if os.path.exists(os.path.dirname(dest_path)):
                    dest_handoff_url = "file://%s" % os.path.abspath(parsed_handoff_url.path)
                else:
                    print "Destination doesn't exist, attempting to use temporary file..."
                    temp_dir = mkdtemp(prefix="cloudlet-residue-")
                    residue_zipfile = os.path.join(temp_dir, Cloudlet_Const.OVERLAY_ZIP)
                    dest_handoff_url = "file://%s" % os.path.abspath(residue_zipfile)
            handoff_ds = handoff.HandoffDataSend()
            LOG.debug("save data to file")
            handoff_ds.save_data(
                base_vm_paths, base_hash,
                preload_thread.basedisk_hashdict,
                preload_thread.basemem_hashdict,
                options, dest_handoff_url, None,
                synthesized_vm.fuse.mountpoint, synthesized_vm.qemu_logfile,
                synthesized_vm.qmp_channel, synthesized_vm.machine.ID(),
                synthesized_vm.fuse.modified_disk_chunks, "qemu:///system",
                title, fwd_ports
            )
            handoff_ds._load_vm_data()
            try:
                handoff.perform_handoff(handoff_ds, op_id)
            except handoff.HandoffError as e:
                LOG.error("Cannot perform VM handoff: %s" % (str(e)))
            # print out residue location
            if residue_zipfile and os.path.exists(residue_zipfile):
                LOG.info("Save new VM overlay at: %s" %
                        (os.path.abspath(residue_zipfile)))
            if save_snapshot:
                dbconn = DBConnector()
                list = dbconn.list_item(table_def.Snapshot)
                for item in list:
                    if os.path.abspath(parsed_handoff_url.path) == item.path:
                        dbconn.del_item(item)
                        break
                _, basevm = PackagingUtil._get_matching_basevm(disk_path=base_diskpath)
                new = table_def.Snapshot(os.path.abspath(parsed_handoff_url.path), basevm.hash_value)
                dbconn.add_item(new)

            update_op(op_id, has_ended=True)
            synthesized_vm.monitor.terminate()
            synthesized_vm.monitor.join()
            synthesized_vm.terminate()
            dbconn = DBConnector()
            list = dbconn.list_item(table_def.Instances)
            for item in list:
                if title == item.title:
                    dbconn.del_item(item)
                    break

    def terminate(self):
        # force terminate when something wrong in handling request
        # do not wait for joinining
        if hasattr(self, 'delta_proc') and self.delta_proc is not None:
            self.delta_proc.finish()
            if self.delta_proc.is_alive():
                self.delta_proc.terminate()
            self.delta_proc = None
        if hasattr(self, 'resumed') and self.resumed_VM is not None:
            self.resumed_VM.terminate()
            self.resumed_VM = None
        if hasattr(self, 'fuse') and self.fuse is not None:
            self.fuse.terminate()
            self.fuse = None
        if hasattr(self, 'overlay_pipe') and os.path.exists(self.overlay_pipe):
            os.unlink(self.overlay_pipe)
        if hasattr(self, 'tmp_overlay_dir') and os.path.exists(self.tmp_overlay_dir):
            shutil.rmtree(self.tmp_overlay_dir)

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
        LOG.info(" - Disable Nagle(No TCP delay)  : %s" \
                % str(self.socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)))
        LOG.info("-"*50)

    def _load_handoff_data(self, filepath):
        handoff_data = HandoffDataRecv.from_file(filepath)
        if handoff_data is None:
            raise StreamSynthesisError("Invalid handoff recv data at %s" % filepath)
        LOG.info("Load handoff data file at %s" % filepath)
        return handoff_data

    def handle_error(self, request, client_address):
        SocketServer.TCPServer.handle_error(self, request, client_address)
        sys.stderr.write("handling error from client %s\n" % (str(client_address)))
        sys.stderr.write(traceback.format_exc())
        self.terminate()
        sys.exit(1)

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

        return ret_list
