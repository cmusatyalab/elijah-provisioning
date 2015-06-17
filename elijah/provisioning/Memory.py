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
import sys
import select
import struct
import tool
import mmap
import subprocess
import time
import multiprocessing
import traceback
import ctypes
from optparse import OptionParser
from hashlib import sha256

import memory_util
from Configuration import Const
from Configuration import VMOverlayCreationMode
from progressbar import AnimatedProgressBar
from delta import DeltaItem
from delta import DeltaList
from delta import Recovered_delta
import process_manager
import log as logging

LOG = logging.getLogger(__name__)


class MemoryError(Exception):
    pass

class Memory(object):
    HASH_FILE_MAGIC = 0x1145511a
    HASH_FILE_VERSION = 0x00000001

    # kvm-qemu constant (version 1.0.0)
    RAM_MAGIC = 0x5145564d
    RAM_VERSION = 0x00000003
    RAM_PAGE_SIZE    =  (1<<12)
    RAM_ID_STRING       =   "pc.ram"
    RAM_ID_LENGTH       =   len(RAM_ID_STRING)
    RAM_SAVE_FLAG_COMPRESS = 0x02
    RAM_SAVE_FLAG_MEM_SIZE = 0x04
    RAM_SAVE_FLAG_PAGE     = 0x08
    RAM_SAVE_FLAG_RAW      = 0x40
    RAM_SAVE_FLAG_EOS      = 0x10
    RAM_SAVE_FLAG_CONTINUE = 0x20
    BLK_MIG_FLAG_EOS       = 0x02

    # header format for each memory page
    CHUNK_HEADER_FMT = "=Q"
    CHUNK_HEADER_SIZE = struct.calcsize("=Q")
    ITER_SEQ_BITS   = 16
    ITER_SEQ_SHIFT  = CHUNK_HEADER_SIZE * 8 - ITER_SEQ_BITS
    CHUNK_POS_MASK   = (1 << ITER_SEQ_SHIFT) - 1
    ITER_SEQ_MASK   = ((1 << (CHUNK_HEADER_SIZE * 8)) - 1) - CHUNK_POS_MASK


    def __init__(self):
        self.hash_list = []
        self.raw_file = ''
        self.raw_filesize = 0
        self.raw_mmap = None

    @staticmethod
    def _seek_string(f, string):
        # return: index of end of the found string
        start_index = f.tell()
        memdata = ''
        while True:
            memdata = f.read(Memory.RAM_PAGE_SIZE)
            if not memdata:
                raise MemoryError("Cannot find %s from give memory snapshot" % Memory.RAM_ID_STRING)

            ram_index = memdata.find(Memory.RAM_ID_STRING)
            if ram_index:
                if ord(memdata[ram_index-1]) == len(string):
                    position = start_index + ram_index
                    f.seek(position)
                    return position
            start_index += len(memdata)

    def _get_mem_hash(self, fin, end_offset, hash_list, **kwargs):
        # kwargs
        #  diff: compare hash_list with self object
        #  free_pfn_dict: free memory physical frame number as a dictionary {'#':1, ... }
        diff = kwargs.get("diff", None)
        apply_free_memory = kwargs.get("apply_free_memory", True)
        free_pfn_dict = kwargs.get("free_pfn_dict", None)
        LOG.info("Get hash list of memory page")
        prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)

        total_size = end_offset
        ram_offset = 0
        freed_page_counter = 0
        base_hashlist_length = len(self.hash_list)
        while total_size != ram_offset:
            data = fin.read(Memory.RAM_PAGE_SIZE)
            if not diff:
                hash_list.append((ram_offset, len(data), sha256(data).digest()))
            else:
                # compare input with hash or corresponding base memory, save only when it is different
                hash_list_index = ram_offset/Memory.RAM_PAGE_SIZE
                if hash_list_index < base_hashlist_length:
                    self_hash_value = self.hash_list[hash_list_index][2]
                else:
                    self_hash_value = None

                if self_hash_value != sha256(data).digest():
                    is_free_memory = False
                    if (free_pfn_dict != None) and \
                            (free_pfn_dict.get(long(ram_offset/Memory.RAM_PAGE_SIZE), None) == 1):
                        is_free_memory = True

                    if is_free_memory and apply_free_memory:
                        # Do not compare. It is free memory
                        freed_page_counter += 1
                    else:
                        #get xdelta comparing self.raw
                        source_data = self.get_raw_data(ram_offset, len(data))
                        #save xdelta as DeltaItem only when it gives smaller
                        try:
                            if source_data == None:
                                raise IOError("launch memory snapshot is bigger than base vm")
                            patch = tool.diff_data(source_data, data, 2*len(source_data))
                            if len(patch) < len(data):
                                delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
                                        ram_offset, len(data),
                                        hash_value=sha256(data).digest(),
                                        ref_id=DeltaItem.REF_XDELTA,
                                        data_len=len(patch),
                                        data=patch)
                            else:
                                raise IOError("xdelta3 patch is bigger than origianl")
                        except IOError as e:
                            #LOG.info("xdelta failed, so save it as raw (%s)" % str(e))
                            delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
                                    ram_offset, len(data),
                                    hash_value=sha256(data).digest(),
                                    ref_id=DeltaItem.REF_RAW,
                                    data_len=len(data),
                                    data=data)
                        hash_list.append(delta_item)

                # memory over-usage protection
                if len(hash_list) > Memory.RAM_PAGE_SIZE*1000000: # 400MB for hashlist
                    raise MemoryError("possibly comparing with wrong base VM")
            ram_offset += len(data)
            # print progress bar for every 100 page
            if (ram_offset % (Memory.RAM_PAGE_SIZE*100)) == 0:
                prog_bar.set_percent(100.0*ram_offset/total_size)
                prog_bar.show_progress()
        prog_bar.finish()
        return freed_page_counter

    @staticmethod
    def _seek_to_end_of_ram(fin):
        # get ram total length
        position = Memory._seek_string(fin, Memory.RAM_ID_STRING)
        memory_start_offset = position-(1+8)
        fin.seek(memory_start_offset)
        total_mem_size = long(struct.unpack(">Q", fin.read(8))[0])
        if total_mem_size & Memory.RAM_SAVE_FLAG_MEM_SIZE == 0:
            raise MemoryError("invalid header format: no total memory size")
        total_mem_size = total_mem_size & ~0xfff

        # get ram length information
        read_ramlen_size = 0
        ram_info = dict()
        while total_mem_size > read_ramlen_size:
            id_string_len = ord(struct.unpack(">s", fin.read(1))[0])
            id_string, mem_size = struct.unpack(">%dsQ" % id_string_len,\
                    fin.read(id_string_len+8))
            ram_info[id_string] = {"length":mem_size}
            read_ramlen_size += mem_size

        read_mem_size = 0
        while total_mem_size != read_mem_size:
            raw_ram_flag = struct.unpack(">Q", fin.read(8))[0]
            if raw_ram_flag & Memory.RAM_SAVE_FLAG_EOS:
                raise MemoryError("Error, Not Fully load yet")
                break
            if raw_ram_flag & Memory.RAM_SAVE_FLAG_RAW == 0:
                raise MemoryError("Error, invalid ram save flag raw\n")

            id_string_len = ord(struct.unpack(">s", fin.read(1))[0])
            id_string = struct.unpack(">%ds" % id_string_len, fin.read(id_string_len))[0]
            padding_len = fin.tell() & (Memory.RAM_PAGE_SIZE-1)
            padding_len = Memory.RAM_PAGE_SIZE-padding_len
            fin.read(padding_len)

            cur_offset = fin.tell()
            block_info = ram_info.get(id_string)
            if not block_info:
                raise MemoryError("Unknown memory block : %s", id_string)
            block_info['offset'] = cur_offset
            memory_size = block_info['length']
            fin.seek(cur_offset + memory_size)
            read_mem_size += memory_size

        return fin.tell(), ram_info


    def _load_file(self, filepath, **kwargs):
        # Load KVM Memory snapshot file and 
        # extract hashlist of each memory page while interpreting the format
        # filepath = file path of the loading file
        # kwargs
        #  diff_file: compare filepath(modified ram) with self hash
        ####
        diff = kwargs.get("diff", None)
        apply_free_memory = kwargs.get("apply_free_memory", True)
        if diff and len(self.hash_list) == 0:
            raise MemoryError("Cannot compare give file this self.hashlist")

        # Sanity check
        fin = open(filepath, "rb")
        file_size = os.path.getsize(filepath)
        libvirt_mem_hdr = memory_util._QemuMemoryHeader(fin)
        libvirt_mem_hdr.seek_body(fin)
        libvirt_header_len = fin.tell()
        if ((libvirt_header_len %  Memory.RAM_PAGE_SIZE) != 0):
            # TODO: need to modify libvirt migration file header 
            # in case it is not aligned with memory page size
            msg = "Error description:\n"
            msg += "libvirt header length : %ld\n" % (libvirt_header_len)
            msg += "This happends when resiude generated multiple times\n"
            msg += "It's not easy to fix since header length change will make VM's memory snapshot size\n"
            msg += "different from base VM"
            raise MemoryError(msg)

        # get memory meta data from snapshot
        fin.seek(libvirt_header_len)
        hash_list = []
        #ram_end_offset, ram_info = Memory._seek_to_end_of_ram(fin)
        #if ram_end_offset == Memory.RAM_PAGE_SIZE:
        #    LOG.debug("end offset: %ld" % (ram_end_offset))
        #    raise MemoryError("ram header+data is not aligned with page size")

        if diff:
            # case for getting modified memory list
            if apply_free_memory == True:
                # get free memory list
                mem_size_mb = ram_info.get('pc.ram').get('length')/1024/1024
                mem_abs_offset = ram_info.get('pc.ram').get('offset')
                self.free_pfn_dict = get_free_pfn_dict(filepath, mem_size_mb, \
                        mem_abs_offset)
            else:
                self.free_pfn_dict = None

            fin.seek(0)
            freed_counter = self._get_mem_hash(fin, file_size, hash_list, \
                    diff=diff, free_pfn_dict=self.free_pfn_dict, \
                    apply_free_memory=apply_free_memory)
        else:
            # case for generating base memory hash list
            fin.seek(0)
            freed_counter = self._get_mem_hash(fin, file_size, hash_list, \
                    diff=diff, free_pfn_dict=None)

        # get hash of memory area
        self.freed_counter = freed_counter
        LOG.debug("FREE Memory Counter: %ld(%ld)" % \
                (freed_counter, freed_counter*Memory.RAM_PAGE_SIZE))
        return hash_list

    @staticmethod
    def import_from_metafile(meta_path, raw_path):
        # Regenerate KVM Base Memory DS from existing meta file
        if (not os.path.exists(raw_path)) or (not os.path.exists(meta_path)):
            msg = "Cannot import from hash file, No raw file at : %s" % raw_path
            raise MemoryError(msg)

        memory = Memory()
        memory.raw_file = open(raw_path, "rb")
        memory.raw_filesize = os.path.getsize(raw_path)
        hashlist = Memory.import_hashlist(meta_path)
        memory.hash_list = hashlist
        return memory

    @staticmethod
    def import_hashlist(meta_path):
        fd = open(meta_path, "rb")

        # Read Hash Item List
        hash_list = list()
        count = 0
        while True:
            count += 1
            data = fd.read(8+4+32) # start_offset, length, hash
            if not data:
                break
            value = tuple(struct.unpack("!qI32s", data))
            hash_list.append(value)
        fd.close()
        return hash_list

    @staticmethod
    def pack_hashlist(hash_list):
        # pack hash list
        original_length = len(hash_list)
        hash_list = dict((x[2], x) for x in hash_list).values()
        LOG.debug("hashlist is packed: from %d to %d : %lf" % \
                (original_length, len(hash_list), 1.0*len(hash_list)/original_length))

    def export_to_file(self, f_path):
        fd = open(f_path, "wb")
        # Write hash item list
        for (start_offset, length, data) in self.hash_list:
            # save it as little endian format
            row = struct.pack("!qI32s", start_offset, length, data)
            fd.write(row)
        fd.close()

    def get_raw_data(self, offset, length):
        # retrieve page data from raw memory
        if not self.raw_mmap:
            self.raw_mmap = mmap.mmap(self.raw_file.fileno(), 0, prot=mmap.PROT_READ)
        if offset+length < self.raw_filesize:
            return self.raw_mmap[offset:offset+length]
        else:
            return None

    def get_modified(self, new_kvm_file, apply_free_memory=True, free_memory_info=None):
        # get modified pages 
        hash_list = self._load_file(new_kvm_file, diff=True, \
                apply_free_memory=apply_free_memory)
        if free_memory_info != None:
            free_memory_info['free_pfn_dict'] = self.free_pfn_dict
            free_memory_info['freed_counter'] = self.freed_counter

        return hash_list


def base_hashlist(base_memmeta_path):
    # get the hash list from the meta file
    hashlist = Memory.import_hashlist(base_memmeta_path)
    return hashlist


def hashing(filepath):
    # Contstuct KVM Base Memory DS from KVM migrated memory
    # filepath  : input KVM Memory Snapshot file path
    memory = Memory()
    hash_list =  memory._load_file(filepath)
    memory.hash_list = hash_list
    return memory


def _process_cmd(argv):
    COMMANDS = ['hashing', 'delta', 'recover']
    USAGE = "Usage: %prog " + "[%s] [option]" % '|'.join(COMMANDS)
    VERSION = '%prog ' + str(1.0)
    DESCRIPTION = "KVM Memory struction interpreste"

    parser = OptionParser(usage=USAGE, version=VERSION, description=DESCRIPTION)
    parser.add_option("-m", "--migrated_file", type="string", dest="mig_file", action='store', \
            help="Migrated file path")
    parser.add_option("-r", "--raw_file", type="string", dest="raw_file", action='store', \
            help="Raw memory path")
    parser.add_option("-s", "--hash_file", type="string", dest="hash_file", action='store', \
            help="Hashsing file path")
    parser.add_option("-d", "--delta", type="string", dest="delta_file", action='store', \
            default="mem_delta", help="path for delta list")
    parser.add_option("-b", "--base", type="string", dest="base_file", action='store', \
            help="path for base memory file")
    settings, args = parser.parse_args()
    if len(args) != 1:
        parser.error("Cannot find command")
    command = args[0]
    if command not in COMMANDS:
        parser.error("Invalid Command: %s, supporing %s" % (command, ' '.join(COMMANDS)))
    return settings, command


def create_memory_deltalist(modified_mempath,
            basemem_meta=None, basemem_path=None,
            apply_free_memory=True,
            free_memory_info=None):
    # get memory delta
    # modified_mempath : file path for modified memory
    # basemem_meta : hashlist file for base mem
    # basemem_path : raw base memory path
    # freed_counter_ret : return pointer for freed counter

    # Create Base Memory from meta file
    base = Memory.import_from_metafile(basemem_meta, basemem_path)

    # 1.get modified page
    LOG.debug("1.get modified page list")
    delta_list = base.get_modified(modified_mempath,
            apply_free_memory=apply_free_memory,
            free_memory_info=free_memory_info)
    return delta_list


class CreateMemoryDeltalist(process_manager.ProcWorker):
#import threading
#class CreateMemoryDeltalist(threading.Thread):
    def __init__(self, modified_mem_queue, deltalist_queue, 
                 basemem_meta, basemem_path, overlay_mode,
                 apply_free_memory=True,
                 free_memory_info=None):
        self.modified_mem_queue = modified_mem_queue
        self.deltalist_queue = deltalist_queue
        self.basemem_meta = basemem_meta
        self.memory_hashlist = Memory.import_hashlist(basemem_meta)
        self.apply_free_memory = apply_free_memory
        self.free_memory_info = free_memory_info
        self.basemem_path = basemem_path
        self.proc_list = list()
        self.overlay_mode = overlay_mode
        self.num_proc = VMOverlayCreationMode.MAX_THREAD_NUM
        self.diff_algorithm = overlay_mode.MEMORY_DIFF_ALGORITHM

        self.monitor_current_iteration = multiprocessing.RawValue(ctypes.c_ulong, 0)

        super(CreateMemoryDeltalist, self).__init__(target=self.create_memory_deltalist)

    def create_memory_deltalist(self):
        # get memory delta
        self.modified_memory_fd = SeekablePipe(self.modified_mem_queue)

        # get modified pages 
        libvirt_mem_hdr = memory_util._QemuMemoryHeader(self.modified_memory_fd)
        libvirt_mem_hdr.seek_body(self.modified_memory_fd)
        libvirt_header_len = self.modified_memory_fd.tell()
        if ((libvirt_header_len %  Memory.RAM_PAGE_SIZE) != 0):
            # TODO: need to modify libvirt migration file header 
            # in case it is not aligned with memory page size
            msg = "Error description:\n"
            msg += "libvirt header length : %ld\n" % (libvirt_header_len)
            msg += "This happends when resiude generated multiple times\n"
            msg += "It's not easy to fix since header length change will make VM's memory snapshot size\n"
            msg += "different from base VM"
            raise MemoryError(msg)

        # get memory meta data from snapshot
        self.modified_memory_fd.seek(libvirt_header_len)

        self.free_pfn_dict = None
        try:
            self.freed_counter = self._get_modified_memory_page(self.modified_memory_fd)
            LOG.debug("FREE Memory Counter: %ld(%ld)" % \
                    (self.freed_counter, self.freed_counter*Memory.RAM_PAGE_SIZE))
            if self.free_memory_info != None:
                self.free_memory_info['free_pfn_dict'] = self.free_pfn_dict
                self.free_memory_info['freed_counter'] = self.freed_counter

        except Exception, e:
            sys.stdout.write("[compression] Exception1n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.deltalist_queue.put(Const.QUEUE_FAILED_MESSAGE)

    def change_mode(self, new_mode):
        for (proc, c_queue, m_queue) in self.proc_list:
            if proc.is_alive() == True:
                m_queue.put(("new_mode", new_mode))

    def _process_libvirt_header(self, libvirt_header_list):
        base_memory_fd = open(self.basemem_path)
        delta_list = list()
        header_in_size = 0
        header_out_size = 0
        for index, libvirt_header_chunk in enumerate(libvirt_header_list):
            data = libvirt_header_chunk
            offset = index*Memory.RAM_PAGE_SIZE
            base_memory_fd.seek(offset)
            base_data = base_memory_fd.read(Memory.RAM_PAGE_SIZE)
            chunk_hashvalue = sha256(data).digest()
            base_hashvalue = sha256(base_data).digest()
            if chunk_hashvalue == base_hashvalue:
                continue
            try:
                if self.diff_algorithm == "xdelta3":
                    diff_data = tool.diff_data(base_data, data, 2*len(base_data))
                    diff_type = DeltaItem.REF_XDELTA
                    if len(diff_data) > len(data):
                        raise IOError("xdelta3 patch is bigger than origianl")
                elif self.diff_algorithm == "bsdiff":
                    diff_data = tool.diff_data_bsdiff(base_data, data)
                    diff_type = DeltaItem.REF_BSDIFF
                    if len(diff_data) > len(data):
                        raise IOError("bsdiff patch is bigger than origianl")
                elif self.diff_algorithm == "xor":
                    diff_data = tool.cython_xor(base_data, data)
                    diff_type = DeltaItem.REF_XOR
                    if len(diff_data) > len(data):
                        raise IOError("xor patch is bigger than origianl")
                elif self.diff_algorithm == "none":
                    diff_data = data
                    diff_type = DeltaItem.REF_RAW
                else:
                    raise MemoryError("%s algorithm is not supported" % self.diff_algorithm)
            except IOError as e:
                diff_data = data
                diff_type = DeltaItem.REF_RAW

            header_in_size += (len(data)+11)
            header_out_size += (len(diff_data)+11)
            delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
                    offset, len(data),
                    hash_value=chunk_hashvalue,
                    ref_id=diff_type,
                    data_len=len(diff_data),
                    data=diff_data)
            delta_list.append(delta_item)
        base_memory_fd.close()
        self.total_block += len(delta_list)
        self.deltalist_queue.put(delta_list)
        return header_in_size, header_out_size

    def chunks(self, l, n):
        ret_chunks = list()
        for index in range(0, len(l), n):
            chunked_data = l[index:index+n]
            chunked_data_size = len(chunked_data)
            if chunked_data_size == n:
                header = chunked_data[0:Memory.CHUNK_HEADER_SIZE]
                blob_offset, = struct.unpack(Memory.CHUNK_HEADER_FMT, header)
                iter_seq = (blob_offset & Memory.ITER_SEQ_MASK) >> Memory.ITER_SEQ_SHIFT
                if iter_seq != self.iteration_seq:
                    msg = "adaptation\tqemu_control\tstart iteration\t%f\t%d\t%d\t%d" % \
                        (time.time(), self.iteration_seq, iter_seq, self.iteration_size)
                    self.iteration_seq = iter_seq
                    LOG.debug(msg)
                    self.iteration_size = 0
                    self.monitor_current_iteration.value = iter_seq
            ret_chunks.append(chunked_data)
            self.iteration_size += chunked_data_size
        return ret_chunks

    @staticmethod
    def averaged_value(measure_hist, cur_time):
        avg_p = float(0)
        avg_r = float(0)
        counter = 0
        for (measured_time, p, r) in reversed(measure_hist):
            if cur_time - measured_time > VMOverlayCreationMode.MEASURE_AVERAGE_TIME:
                break
            avg_p += p
            avg_r += r
            counter += 1
        #self.measure_history = self.measure_history[-1*(counter+1):]
        #LOG.debug("%f measure last %d/%d" % (time.time(), counter, len(self.measure_history)))
        return avg_p/counter, avg_r/counter

    def _get_modified_memory_page(self, fin):

        time_s = time.time()
        LOG.info("Get hash list of memory page")
        is_first_recv = False
        time_first_recv = 0

        # measurement
        self.measure_history = list()
        self.measure_history_cur = list()
        self.total_block = 0
        self.total_time = float(0)
        self.iteration_datasize_list = list()
        self.iteration_size = 0
        self.iteration_seq = 0

        # Due to the header of each memory page, memory chunk size is not 4KB +
        # 8 bytes. Also, we need to follow this format for libvirt header.
        memory_chunk_size = Memory.CHUNK_HEADER_SIZE + Memory.RAM_PAGE_SIZE
        fin.seek(Const.LIBVIRT_HEADER_SIZE*2) # make sure to have data bigger than libvirt header

        # process libvirt header first
        libvirt_header_data = fin.data_buffer[:Const.LIBVIRT_HEADER_SIZE]
        libvirt_header_list = list()
        for index in range(0, len(libvirt_header_data), Memory.RAM_PAGE_SIZE):
            chunked_data = libvirt_header_data[index:index+Memory.RAM_PAGE_SIZE]
            libvirt_header_list.append(chunked_data)
        libvirt_header_offset = len(libvirt_header_list)*Memory.RAM_PAGE_SIZE
        time_header_start = time.time()
        header_in_size, header_out_size = self._process_libvirt_header(libvirt_header_list)
        self.in_size += header_in_size
        self.out_size += header_out_size
        time_header_end = time.time()

        memory_page_list = list()
        memory_data = fin.data_buffer[Const.LIBVIRT_HEADER_SIZE:]
        memory_page_list += self.chunks(memory_data, memory_chunk_size)
        memory_data_queue = fin.data_queue

        # launch child processes
        output_fd_list = list()
        base_hashlist_length = len(self.memory_hashlist)
        self.task_queue = multiprocessing.Queue(maxsize=VMOverlayCreationMode.MAX_THREAD_NUM)
        for i in range(self.num_proc):
            command_queue = multiprocessing.Queue()
            mode_queue = multiprocessing.Queue()
            diff_proc = MemoryDiffProc(command_queue, self.task_queue, mode_queue,
                                       self.deltalist_queue,
                                       self.diff_algorithm,
                                       self.basemem_path,
                                       base_hashlist_length,
                                       self.memory_hashlist,
                                       libvirt_header_offset,
                                       self.free_pfn_dict,
                                       self.apply_free_memory)
            diff_proc.start()
            self.proc_list.append((diff_proc, command_queue, mode_queue))

        recved_data_size = 0
        freed_page_counter = 0
        is_end_of_stream = False
        while is_end_of_stream == False and len(memory_page_list) != 0:
            # get data from the stream
            if len(memory_page_list) < 2: # empty or partial data

                input_fd = [self.control_queue._reader.fileno(), memory_data_queue._reader.fileno()]
                input_ready, out_ready, err_ready = select.select(input_fd, [], [])
                if self.control_queue._reader.fileno() in input_ready:
                    control_msg = self.control_queue.get()
                    ret = self._handle_control_msg(control_msg)
                    if ret == False:
                        if control_msg == "change_mode":
                            new_mode = self.control_queue.get()
                            self.change_mode(new_mode)

                if memory_data_queue._reader.fileno() in input_ready:
                    recved_data = memory_data_queue.get()
                    if is_first_recv == False:
                        is_first_recv = True
                        time_first_recv = time.time()

                    recved_data_size = len(recved_data)
                    if recved_data == Const.QUEUE_SUCCESS_MESSAGE:
                        # End of the stream
                        is_end_of_stream = True
                        continue
                    else:
                        required_length = 0
                        if len(memory_page_list) == 1: # handle partial data
                            last_data = memory_page_list.pop(0)
                            required_length = memory_chunk_size-len(last_data)
                            last_data += recved_data[0:required_length]
                            memory_page_list.append(last_data)
                        memory_page_list += self.chunks(recved_data[required_length:], memory_chunk_size)

            if len(memory_page_list) > 1:
                tasks = memory_page_list[0:-1]
                memory_page_list = memory_page_list[-1:]
                self.task_queue.put(tasks)

            total_process_time = 0
            total_block_count = 0
            total_input_size = 0
            total_output_size = 0
            for (proc, c_queue, mode_queue) in self.proc_list:
                process_time = proc.child_process_time_total.value
                block_count = proc.child_process_block_total.value
                input_size = proc.child_input_size_total.value
                output_size = proc.child_output_size_total.value

                total_process_time += process_time
                total_block_count += block_count
                total_input_size += input_size
                total_output_size += output_size

            # record only when there's an update
            prev_measure_values = (0, 0, 0, 0, 0)
            if len(self.measure_history) >= 1:
                prev_measure_values = self.measure_history[-1]
            prev_m_time, prev_process_time, prev_block_count, prev_insize, prev_outsize = prev_measure_values
            monitor_insize = total_input_size + header_in_size
            monitor_outsize = total_output_size + header_out_size
            if (prev_process_time < total_process_time) and (prev_block_count < total_block_count)\
                    and (monitor_insize > prev_insize) and (monitor_outsize > prev_outsize):
                monitor_p = total_process_time/total_block_count
                monitor_r = float(total_output_size)/total_input_size
                self.monitor_total_time_block.value = monitor_p
                self.monitor_total_ratio_block.value = monitor_r
                self.monitor_total_input_size.value = monitor_insize
                self.monitor_total_output_size.value = monitor_outsize
                cur_wall_time = time.time()
                self.measure_history.append((cur_wall_time, total_process_time, total_block_count, monitor_insize, monitor_outsize))

                # get cur value compared to prev value
                cur_process_time = total_process_time - prev_process_time
                cur_block_count = total_block_count - prev_block_count
                cur_insize = monitor_insize - prev_insize
                cur_outsize = monitor_outsize - prev_outsize
                cur_p = cur_process_time/cur_block_count
                cur_r = float(cur_outsize)/cur_insize
                self.measure_history_cur.append((cur_wall_time, cur_p, cur_r))
                avg_cur_p, avg_cur_r = self.averaged_value(self.measure_history_cur, cur_wall_time)

                self.monitor_total_time_block_cur.value = avg_cur_p
                self.monitor_total_ratio_block_cur.value = avg_cur_r
                self.monitor_total_input_size_cur.value = cur_insize
                self.monitor_total_output_size_cur.value = cur_outsize
                #LOG.debug("%f\t%f\tprocess:%s, %s, %s\tcur_block:%s, %s, %s\t" % \
                #          (cur_p, avg_cur_p,
                #           total_process_time, prev_process_time, cur_process_time,
                #           total_block_count, prev_block_count, cur_block_count))

        # send last memory page
        # libvirt randomly add string starting with 'LibvirtQemudSave'
        # Therefore, process the last memory page only when it's aligned
        if len(memory_page_list) > 0 and len(memory_page_list[0]) == (Memory.RAM_PAGE_SIZE + Memory.CHUNK_HEADER_SIZE):
            LOG.debug("[Memory][child] send last data to child: %d" % len(memory_page_list))
            #select.select([], [self.task_queue._writer.fileno()], [])
            self.task_queue.put(memory_page_list)
        self.finish_processing_input.value = True

        # send end meesage to every process
        for child_proc in self.proc_list:
            LOG.debug("[Memory] send end message to each child")
            #select.select([], [self.task_queue._writer.fileno()], [])
            self.task_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        # after this for loop, all processing finished, but child process still
        # alive until all data pass to the next step
        finished_proc_dict = dict()
        input_list = [self.control_queue._reader.fileno()]
        for (proc, c_queue, mode_queue) in self.proc_list:
            fileno = c_queue._reader.fileno()
            input_list.append(fileno)
            finished_proc_dict[fileno] = c_queue
        while len(finished_proc_dict.keys()) > 0:
            (input_ready, [], []) = select.select(input_list, [], [], 0.01)
            for in_queue in input_ready:
                if self.control_queue._reader.fileno() == in_queue:
                    control_msg = self.control_queue.get()
                    self._handle_control_msg(control_msg)
                else:
                    cq = finished_proc_dict[in_queue]
                    (input_size, output_size, blocks, processed_time) = cq.get()
                    self.in_size += input_size
                    self.total_block += blocks
                    self.out_size += output_size
                    self.total_time += processed_time
                    del finished_proc_dict[in_queue]
        self.is_processing_alive.value = False
        time_e = time.time()
        LOG.debug("profiling\t%s\tsize\t%ld\t%ld\t%f" % (self.__class__.__name__,
                                                         self.in_size,
                                                         self.out_size,
                                                         (self.out_size/float(self.in_size))))
        LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f\t%f" %\
                  (self.__class__.__name__, time_s, time_e,\
                   (time_e-time_s), self.total_time))
        LOG.debug("profiling\t%s\tblock-size\t%f\t%f\t%d" % (self.__class__.__name__,
                                                         float(self.in_size)/self.total_block,
                                                         float(self.out_size)/self.total_block,
                                                         self.total_block))
        LOG.debug("profiling\t%s\tblock-time\t%f\t%f\t%f" %\
                  (self.__class__.__name__, time_s, time_e, self.total_time/self.total_block))

        for (proc, c_queue, mode_queue) in self.proc_list:
            #LOG.debug("[Memory] waiting to dump all data to the next stage")
            proc.join()
        # send end message after the next stage finishes processing
        self.deltalist_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        LOG.debug("[time] Memory xdelta first input at : %f" % (time_first_recv))
        return freed_page_counter


def recover_memory(base_disk, base_mem, delta_path, out_path, verify_with_original=None):
    # Recover modified memory snapshot
    # base_path: base memory snapshot, delta pages will be applied over it
    # delta_path: memory overlay
    # out_path: path to recovered modified memory snapshot
    # verify_with_original: original modification file for recover verification

    recovered_memory = Recovered_delta(base_disk, base_mem, delta_path, out_path, \
            Memory.RAM_PAGE_SIZE, parent=base_mem)

    chunk_list = []
    for chunk_number in recovered_memory.recover_chunks():
        chunk_list.append("%ld:1" % chunk_number)
    recovered_memory.finish()

    # varify with original
    if verify_with_original:
        modi_mem = open(verify_with_original, "rb")
        base_file = open(base_mem, "rb")
        delta_list_index = 0
        while True:
            offset = base_file.tell()
            if len(delta_list) == delta_list_index:
                break

            base_data = base_file.read(Memory.RAM_PAGE_SIZE)
            if offset != delta_list[delta_list_index].offset:
                #LOG.debug("from base data: %d" % len(base_data))
                modi_mem.seek(offset)
                modi_data = modi_mem.read(len(base_data))
                if modi_data != base_data:
                    msg = "orignal data is not same at %ld" % offset
                    raise MemoryError(msg)
            else:
                modi_mem.seek(offset)
                recover_data = delta_list[delta_list_index].data
                origin_data = modi_mem.read(len(recover_data))
                #LOG.debug("from recovered data: %d at %ld" % (len(recover_data), delta_list[delta_list_index].offset))
                delta_list_index += 1
                if recover_data != origin_data:
                    msg = "orignal data is not same at %ld" % offset
                    raise MemoryError(msg)

        for delta_item in delta_list:
            offset = delta_item.offset
            data = delta_item.data
            modi_mem.seek(offset)
            origin_data = modi_mem.read(len(data))
            if data != origin_data:
                msg = "orignal data is not same at %ld" % offset
                raise MemoryError(msg)
        LOG.debug("Pass all varification - Successfully recovered")

    return ','.join(chunk_list)


def get_free_pfn_dict(snapshot_path, mem_size, mem_offset):
    if mem_size == 1024:
        pglist_addr = 'c1840a80'
        pgn0_addr = 'f73fd000'
    elif mem_size == 2048:
        pglist_addr = 'c1840a80'
        pgn0_addr = 'f553c000'
    else:
        LOG.error("Error, memory size %ld KB is not valid" % (mem_size))
        return None

    free_pfn_list = _get_free_pfn_list(snapshot_path, pglist_addr, pgn0_addr, \
            mem_size, mem_offset)
    if free_pfn_list:
        # free_pfn_list starts from the pc.ram so add offset of of pc.ram
        offset = (mem_offset)/Memory.RAM_PAGE_SIZE
        free_pfn_aligned = dict([(long(page)+offset, True) for page in free_pfn_list])
        return free_pfn_aligned
    else:
        return None


def _get_free_pfn_list(snapshot_path, pglist_addr, pgn0_addr, mem_size_gb, mem_offset):
    # get list of free memory page number
    BIN_PATH = Const.FREE_MEMORY_BIN_PATH
    cmd = [
            "%s" % BIN_PATH,
            "%s" % snapshot_path,
            "%s" % pglist_addr,
            "%s" % pgn0_addr,
            "%d" % mem_size_gb,
            "%d" % mem_offset,
        ]
    _PIPE = subprocess.PIPE
    LOG.info("Start getting free memory pages")
    proc = subprocess.Popen(cmd, close_fds=True, stdin=_PIPE, stdout=_PIPE, stderr=_PIPE)
    out, err = proc.communicate()
    if err:
        LOG.warning("Error in getting free memory : %s" % str(err))
        return list()
    free_pfn_list = out.split("\n")
    if len(free_pfn_list[-1].strip()) == 0:
        free_pfn_list = free_pfn_list[:-1]
    LOG.info("Free memory pages : %ld" % len(free_pfn_list))
    LOG.info("Finish getting free memory pages")
    return free_pfn_list


class SeekablePipe(object):
    def __init__(self, data_queue):
        self.data_queue = data_queue
        self.current_data_size = 0
        self.current_seek_offset = 0
        self.data_buffer = ""
        self.closed = False

    def seek(self, abs_offset):
        while abs_offset > self.current_data_size:
            select.select([self.data_queue._reader.fileno()], [], [])
            data = self.data_queue.get()
            if len(data) == Const.QUEUE_SUCCESS_MESSAGE_LEN and data == Const.QUEUE_SUCCESS_MESSAGE:
                self.closed = True
                break
            self.data_buffer += data
            self.current_data_size += len(data)
        self.current_seek_offset = abs_offset

    def read(self, read_size):
        read_offset = self.current_seek_offset + read_size
        while self.current_data_size < read_offset:
            select.select([self.data_queue._reader.fileno()], [], [])
            data = self.data_queue.get()
            if len(data) == Const.QUEUE_SUCCESS_MESSAGE_LEN and data == Const.QUEUE_SUCCESS_MESSAGE:
                self.closed == True
                break
            self.data_buffer += data
            self.current_data_size += len(data)
        end_offset = min(self.current_data_size, read_offset)
        ret_data = self.data_buffer[self.current_seek_offset:end_offset]
        self.current_seek_offset += len(ret_data)
        return ret_data

    def tell(self):
        return self.current_seek_offset


class MemoryDiffProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, mode_queue, deltalist_queue,
                 diff_algorithm, basemem_path, base_hashlist_length, 
                 memory_hashlist, libvirt_header_offset,
                 free_pfn_dict, apply_free_memory):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.mode_queue = mode_queue
        self.deltalist_queue = deltalist_queue
        self.diff_algorithm = diff_algorithm
        self.basemem_path = basemem_path
        self.base_hashlist_length = base_hashlist_length
        self.memory_hashlist = memory_hashlist
        self.libvirt_header_offset = libvirt_header_offset
        self.free_pfn_dict = free_pfn_dict
        self.apply_free_memory = apply_free_memory

        # shared variables between processes
        self.child_process_time_total = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_process_block_total = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_input_size_total = multiprocessing.RawValue(ctypes.c_ulong, 0)
        self.child_output_size_total = multiprocessing.RawValue(ctypes.c_ulong, 0)

        super(MemoryDiffProc, self).__init__(target=self.process_diff)

    def process_diff(self):
        #self.memory_offset_list = list()
        self.raw_file = open(self.basemem_path, "rb")
        self.raw_mmap = mmap.mmap(self.raw_file.fileno(), 0, prot=mmap.PROT_READ)
        self.raw_filesize = os.path.getsize(self.basemem_path)

        time_process_total_time = float(0)
        child_total_block = 0
        indata_size = 0
        outdata_size = 0
        is_proc_running = True
        input_list = [self.task_queue._reader.fileno(),
                      self.mode_queue._reader.fileno()]
        freed_page_counter = 0
        while is_proc_running:
            #LOG.debug("[Memory][Child] %d waiting on select" % int(os.getpid()))
            inready, outread, errready = select.select(input_list, [], [])
            if self.mode_queue._reader.fileno() in inready:
                # change mode
                (command, value) = self.mode_queue.get()
                if command == "new_mode":
                    new_mode = value
                    new_diff_algorithm = new_mode.get("diff_algorithm", None)
                    if new_diff_algorithm is not None:
                        LOG.debug("change-mode\t%fmemory\t%s -> %s" %\
                                (time.time(), self.diff_algorithm, new_diff_algorithm))
                        self.diff_algorithm = new_diff_algorithm
                elif command == "new_num_cores":
                    new_num_cores = value
                    #print "[memory] child receives new num cores: %s" % (new_num_cores)
                    if new_num_cores is not None:
                        VMOverlayCreationMode.set_num_cores(new_num_cores)
            if self.task_queue._reader.fileno() in inready:
                memory_chunk_list = self.task_queue.get()
                #LOG.debug("[Memory][Child] %d getting a new job: %s %d" %\ (int(os.getpid()), type(memory_chunk_list), len(memory_chunk_list)))
                if memory_chunk_list == Const.QUEUE_SUCCESS_MESSAGE:
                    LOG.debug("[Memory][Child] %d diff proc get end message" % (int(os.getpid())))
                    is_proc_running = False
                    break

                time_process_start = time.clock()
                deltaitem_list = list()
                child_cur_block_count = 0
                indata_size_cur = 0
                outdata_size_cur = 0
                if type(memory_chunk_list) == type(1):
                    LOG.error("Invalid data at memory_chunk_list: %d" % memory_chunk_list)
                    continue
                for data in memory_chunk_list:
                    # header parsing
                    ram_offset, = struct.unpack(Memory.CHUNK_HEADER_FMT, data[0:Memory.CHUNK_HEADER_SIZE])
                    iter_seq = (ram_offset & Memory.ITER_SEQ_MASK) >> Memory.ITER_SEQ_SHIFT
                    ram_offset = (ram_offset & Memory.CHUNK_POS_MASK) + self.libvirt_header_offset
                    #print "%d, %ld" % (iter_seq, ram_offset)

                    # get data
                    data = data[Memory.CHUNK_HEADER_SIZE:]
                    chunk_data_len = len(data)
                    hash_list_index = ram_offset/Memory.RAM_PAGE_SIZE
                    #print "%d\t%d\t%d" % (self.libvirt_header_offset, ram_offset, len(data))

                    is_modified = True
                    chunk_hashvalue = sha256(data).digest()
                    if iter_seq == 0: # compare with base VM if it's the first iteration
                        self_hash_value = None
                        if hash_list_index < self.base_hashlist_length:
                            self_hash_value = self.memory_hashlist[hash_list_index][2]
                        if self_hash_value == chunk_hashvalue:
                            is_modified = False
                        delta_type = DeltaItem.DELTA_MEMORY
                    else:
                        delta_type = DeltaItem.DELTA_MEMORY_LIVE

                    if is_modified == True:
                        #self.memory_offset_list.append((iter_seq, ram_offset))
                        try:
                            # get diff compared to the base VM
                            source_data = self.get_raw_data(ram_offset, len(data))
                            if source_data == None:
                                msg = "launch memory snapshot is bigger than base vm at %ld (%ld > %ld)" %\
                                    (ram_offset, ram_offset+chunk_data_len, self.raw_filesize)
                                #LOG.debug(msg)
                                raise IOError(msg)
                            if self.diff_algorithm == "xdelta3":
                                diff_data = tool.diff_data(source_data, data, 2*len(source_data))
                                diff_type = DeltaItem.REF_XDELTA
                                if len(diff_data) > chunk_data_len:
                                    raise IOError("xdelta3 patch is bigger than origianl")
                            elif self.diff_algorithm == "bsdiff":
                                diff_data = tool.diff_data_bsdiff(source_data, data)
                                diff_type = DeltaItem.REF_BSDIFF
                                if len(diff_data) > chunk_data_len:
                                    raise IOError("bsdiff patch is bigger than origianl")
                            elif self.diff_algorithm == "xor":
                                diff_data = tool.cython_xor(source_data, data)
                                diff_type = DeltaItem.REF_XOR
                                if len(diff_data) > len(data):
                                    raise IOError("xor patch is bigger than origianl")
                            elif self.diff_algorithm == "none":
                                diff_data = data
                                diff_type = DeltaItem.REF_RAW
                            else:
                                diff_data = data
                                diff_type = DeltaItem.REF_RAW
                        except IOError as e:
                            diff_data = data
                            diff_type = DeltaItem.REF_RAW

                        diff_data_len = len(diff_data)
                        indata_size_cur += (chunk_data_len+11)
                        outdata_size_cur += (diff_data_len+11)
                        child_cur_block_count += 1
                        delta_item = DeltaItem(delta_type,
                                               ram_offset, chunk_data_len,
                                               hash_value=chunk_hashvalue,
                                               ref_id=diff_type,
                                               data_len=diff_data_len,
                                               data=diff_data,
                                               live_seq=iter_seq)
                        deltaitem_list.append(delta_item)
                        #print "deltaitem: %d %d" % (diff_type, len(diff_data))
                time_process_end = time.clock()

                time_process_cur_time = (time_process_end - time_process_start)
                child_total_block += child_cur_block_count
                time_process_total_time += time_process_cur_time
                indata_size += indata_size_cur
                outdata_size += outdata_size_cur
                if child_cur_block_count > 0:
                    self.child_input_size_total.value = indata_size
                    self.child_output_size_total.value = outdata_size

                    self.child_process_time_total.value = 1000.0*time_process_total_time
                    self.child_process_block_total.value = child_total_block
                    #LOG.debug("child process time: %s %s" % (time_process_total_time, child_total_block))

                if len(deltaitem_list) > 0:
                    self.deltalist_queue.put(deltaitem_list)
        LOG.debug("[Memory][Child] Child finished. process %d jobs (%f)" % (child_total_block, time_process_total_time))
        self.command_queue.put((indata_size, outdata_size, child_total_block, time_process_total_time))
        #self.task_queue.put(freed_page_counter)
        #out_fd.close()  # measurement
        while self.mode_queue.empty() == False:
            self.mode_queue.get_nowait()
            msg = "Empty new compression mode that does not refelected"
            sys.stdout.write(msg)

    def get_raw_data(self, offset, length):
        # retrieve page data from raw memory
        if offset+length < self.raw_filesize:
            return self.raw_mmap[offset:offset+length]
        else:
            return None


if __name__ == "__main__":
    EXT_META = "-meta"
    settings, command = _process_cmd(sys.argv)

    if command == "hashing":
        if not settings.base_file:
            sys.stderr.write("Error, Cannot find migrated file. See help\n")
            sys.exit(1)
        infile = settings.base_file
        base = hashing(infile)
        base.export_to_file(infile+EXT_META)

        # Check Integrity
        re_base = Memory.import_from_metafile(infile+".meta", infile)
        for index, hashitem in enumerate(re_base.hash_list):
            if base.hash_list[index] != hashitem:
                raise MemoryError("footer data is different")
        LOG.info("meta file information is matched with original")
    elif command == "delta":
        if (not settings.mig_file) or (not settings.base_file):
            sys.stderr.write("Error, Cannot find modified memory file. See help\n")
            sys.exit(1)
        raw_path = settings.base_file
        meta_path = settings.base_file + EXT_META
        modi_mem_path = settings.mig_file
        out_path = settings.mig_file + ".delta"
        #delta_list = create_memory_overlay(modi_mem_path, raw_path, \
        #        modi_mem_path, out_path)

        mem_deltalist= create_memory_deltalist(modi_mem_path,
                basemem_meta=meta_path, basemem_path=raw_path)
        DeltaList.statistics(mem_deltalist)
        DeltaList.tofile(mem_deltalist, modi_mem_path + ".delta")

    elif command == "recover":
        if (not settings.base_file) or (not settings.delta_file):
            sys.stderr.write("Error, Cannot find base/delta file. See help\n")
            sys.exit(1)
        base_mem = settings.base_file
        overlay_mem = settings.delta_file
        base_memmeta = settings.base_file + EXT_META

        out_path = base_mem + ".recover"
        memory_overlay_map = recover_memory(None, base_mem, overlay_mem, \
                base_memmeta, out_path, verify_with_original="./tmp/modi")

