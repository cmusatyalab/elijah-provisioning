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
from optparse import OptionParser
from hashlib import sha256

import memory_util
from Configuration import Const
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

    #def _get_mem_hash(self, fin, deltalist_queue,
    #                  apply_free_memory, free_pfn_dict):
    #    LOG.info("Get hash list of memory page")
    #    #prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)

    #    # data structure to handle pipelined data
    #    def chunks(l, n):
    #        return [l[i:i + n] for i in range(0, len(l), n)]
    #    memory_data = fin.data_buffer
    #    memory_data_queue = fin.data_queue
    #    memory_page_list = chunks(memory_data, Memory.RAM_PAGE_SIZE)

    #    ram_offset = 0
    #    freed_page_counter = 0
    #    base_hashlist_length = len(self.hash_list)
    #    is_end_of_stream = False
    #    time_s = time.time()
    #    while is_end_of_stream == False and len(memory_page_list) != 0:
    #        # get data from the stream
    #        if len(memory_page_list) < 2: # empty or partial data
    #            recved_data = memory_data_queue.get()
    #            if len(recved_data) == Const.QUEUE_SUCCESS_MESSAGE_LEN and recved_data == Const.QUEUE_SUCCESS_MESSAGE:
    #                # End of the stream
    #                is_end_of_stream = True
    #            else:
    #                required_length = 0
    #                if len(memory_page_list) == 1: # handle partial data
    #                    last_data = memory_page_list.pop(0)
    #                    required_length = Memory.RAM_PAGE_SIZE-len(last_data)
    #                    last_data += recved_data[0:required_length]
    #                    memory_page_list.append(last_data)
    #                memory_page_list += chunks(recved_data[required_length:], Memory.RAM_PAGE_SIZE)
    #        data = memory_page_list.pop(0)

    #        # compare input with hash or corresponding base memory, save only when it is different
    #        hash_list_index = ram_offset/Memory.RAM_PAGE_SIZE
    #        if hash_list_index < base_hashlist_length:
    #            self_hash_value = self.hash_list[hash_list_index][2]
    #        else:
    #            self_hash_value = None
    #        chunk_hashvalue = sha256(data).digest()
    #        if self_hash_value != chunk_hashvalue:
    #            is_free_memory = False
    #            if (free_pfn_dict != None) and \
    #                    (free_pfn_dict.get(long(ram_offset/Memory.RAM_PAGE_SIZE), None) == 1):
    #                is_free_memory = True

    #            if is_free_memory and apply_free_memory:
    #                # Do not compare. It is free memory
    #                freed_page_counter += 1
    #            else:
    #                #get xdelta comparing self.raw
    #                source_data = self.get_raw_data(ram_offset, len(data))
    #                try:
    #                    if source_data == None:
    #                        raise IOError("launch memory snapshot is bigger than base vm")
    #                    patch = tool.diff_data(source_data, data, 2*len(source_data))
    #                    if len(patch) < len(data):
    #                        delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
    #                                ram_offset, len(data),
    #                                hash_value=chunk_hashvalue,
    #                                ref_id=DeltaItem.REF_XDELTA,
    #                                data_len=len(patch),
    #                                data=patch)
    #                    else:
    #                        raise IOError("xdelta3 patch is bigger than origianl")
    #                except IOError as e:
    #                    #LOG.info("xdelta failed, so save it as raw (%s)" % str(e))
    #                    delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
    #                            ram_offset, len(data),
    #                            hash_value=chunk_hashvalue,
    #                            ref_id=DeltaItem.REF_RAW,
    #                            data_len=len(data),
    #                            data=data)
    #                '''
    #                delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
    #                        ram_offset, len(data),
    #                        hash_value=chunk_hashvalue,
    #                        ref_id=DeltaItem.REF_RAW,
    #                        data_len=len(data),
    #                        data=data)
    #                '''

    #                deltalist_queue.put(delta_item)

    #        # memory over-usage protection
    #        ram_offset += len(data)
    #        # print progress bar for every 100 page
    #        '''
    #        if (ram_offset % (Memory.RAM_PAGE_SIZE*100)) == 0:
    #            prog_bar.set_percent(100.0*ram_offset/total_size)
    #            prog_bar.show_progress()
    #        '''
    #    #prog_bar.finish()
    #    time_e = time.time()
    #    deltalist_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
    #    return freed_page_counter

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
        # Skip since it prevent streaming of memory snapshot
        '''
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
        '''
        return ram_info

    def _load_file(self, fin, **kwargs):
        apply_free_memory = kwargs.get("apply_free_memory", True)

        # Sanity check
        #file_size = os.path.getsize(filepath)
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
        ram_info = Memory._seek_to_end_of_ram(fin)

        freed_counter = 0
        freed_counter = self._get_mem_hash(fin)

        # get hash of memory area
        self.freed_counter = freed_counter
        LOG.debug("FREE Memory Counter: %ld(%ld)" % \
                (freed_counter, freed_counter*Memory.RAM_PAGE_SIZE))

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

    def get_modified(self, modified_memory_fd, deltalist_queue, 
                     apply_free_memory=True, free_memory_info=None):

        # get modified pages 
        libvirt_mem_hdr = memory_util._QemuMemoryHeader(modified_memory_fd)
        libvirt_mem_hdr.seek_body(modified_memory_fd)
        libvirt_header_len = modified_memory_fd.tell()
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
        modified_memory_fd.seek(libvirt_header_len)
        ram_info = Memory._seek_to_end_of_ram(modified_memory_fd)

        # TODO: support getting free memory for streaming case
        free_pfn_dict = None
        '''
        if apply_free_memory == True:
            # get free memory list
            mem_size_mb = ram_info.get('pc.ram').get('length')/1024/1024
            mem_abs_offset = ram_info.get('pc.ram').get('offset')
            free_pfn_dict = get_free_pfn_dict(filepath, mem_size_mb,
                    mem_abs_offset)
        else:
            free_pfn_dict = None
        '''

        freed_counter = 0
        freed_counter = self._get_mem_hash(modified_memory_fd, deltalist_queue,
                                           apply_free_memory, free_pfn_dict)

        LOG.debug("FREE Memory Counter: %ld(%ld)" % \
                (freed_counter, freed_counter*Memory.RAM_PAGE_SIZE))
        if free_memory_info != None:
            free_memory_info['free_pfn_dict'] = free_pfn_dict
            free_memory_info['freed_counter'] = freed_counter


def hashing(filepath):
    # Contstuct KVM Base Memory DS from KVM migrated memory
    # filepath  : input KVM Memory Snapshot file path
    memory = Memory()
    hash_list =  memory._load_file(open(filepath, 'rb'))
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


class CreateMemoryDeltalist(process_manager.ProcWorker):
    def __init__(self, modified_mem_queue, deltalist_queue, 
                 basemem_meta=None, basemem_path=None,
                 num_proc = 1,
                 diff_algorithm = "xdelta3",
                 apply_free_memory=True,
                 free_memory_info=None):
        self.modified_mem_queue = modified_mem_queue
        self.deltalist_queue = deltalist_queue
        #self.memory_hashdict = memory_hashdict
        self.basemem_meta = basemem_meta
        self.memory_hashlist = Memory.import_hashlist(basemem_meta)
        self.apply_free_memory = apply_free_memory
        self.free_memory_info = free_memory_info
        self.basemem_path = basemem_path
        self.proc_list = list()
        self.num_proc = num_proc
        self.diff_algorithm = diff_algorithm

        # output
        self.prev_procssed_size = 0
        self.prev_procssed_tome = 0
        super(CreateMemoryDeltalist, self).__init__(target=self.create_memory_deltalist)

    def create_memory_deltalist(self):
        # get memory delta
        time_s = time.time()
        LOG.debug("1.get modified page list")
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
        ram_info = Memory._seek_to_end_of_ram(self.modified_memory_fd)

        # TODO: support getting free memory for streaming case
        self.free_pfn_dict = None
        '''
        if self.apply_free_memory == True:
            # get free memory list
            mem_size_mb = ram_info.get('pc.ram').get('length')/1024/1024
            mem_abs_offset = ram_info.get('pc.ram').get('offset')
            free_pfn_dict = get_free_pfn_dict(filepath, mem_size_mb,
                    mem_abs_offset)
        else:
            free_pfn_dict = None
        '''
        try:
            self.freed_counter = self._get_modified_memory_page(self.modified_memory_fd)
            LOG.debug("FREE Memory Counter: %ld(%ld)" % \
                    (self.freed_counter, self.freed_counter*Memory.RAM_PAGE_SIZE))
            if self.free_memory_info != None:
                self.free_memory_info['free_pfn_dict'] = self.free_pfn_dict
                self.free_memory_info['freed_counter'] = self.freed_counter

            time_e = time.time()
            LOG.debug("[time] Memory hashing and diff time (%f ~ %f): %f" % (time_s, time_e, (time_e-time_s)))
        except Exception, e:
            sys.stdout.write("[compression] Exception1n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.deltalist_queue.put(Const.QUEUE_FAILED_MESSAGE)

    def change_mode(self, new_mode):
        for (proc, t_queue, c_queue, m_queue) in self.proc_list:
            if proc.is_alive() == True:
                m_queue.put(new_mode)

    def _get_modified_memory_page(self, fin):
        time_s = time.time()
        LOG.info("Get hash list of memory page")
        is_first_recv = False
        time_first_recv = 0

        # measurement
        processed_datasize = 0
        processed_duration = 0
        time_process_finish = 0
        time_process_start = 0
        time_prev_report = 0
        UPDATE_PERIOD = self.process_info['update_period']

        # data structure to handle pipelined data
        def chunks(l, n):
            return [l[i:i + n] for i in range(0, len(l), n)]
        memory_data = fin.data_buffer
        memory_data_queue = fin.data_queue
        memory_page_list = chunks(memory_data, Memory.RAM_PAGE_SIZE)

        # launch child processes
        base_hashlist_length = len(self.memory_hashlist)
        for i in range(self.num_proc):
            command_queue = multiprocessing.Queue()
            task_queue = multiprocessing.Queue()
            mode_queue = multiprocessing.Queue()
            diff_proc = DiffProc(command_queue, task_queue, mode_queue, self.deltalist_queue,
                                 self.diff_algorithm, self.basemem_path, base_hashlist_length,
                                 self.memory_hashlist, self.free_pfn_dict, self.apply_free_memory)
            diff_proc.start()
            self.proc_list.append((diff_proc, task_queue, command_queue, mode_queue))

        ram_offset = 0
        recved_data_size = 0
        freed_page_counter = 0
        is_end_of_stream = False
        proc_rr_index = 0
        while is_end_of_stream == False and len(memory_page_list) != 0:
            # get data from the stream
            if len(memory_page_list) < 2: # empty or partial data
                # measurement
                if recved_data_size > 0:
                    processed_datasize += recved_data_size
                    time_process_finish = time.time()
                    processed_duration += (time_process_finish - time_process_start)
                    if (time_process_finish - time_prev_report) > UPDATE_PERIOD:
                        time_prev_report = time_process_finish
                        #self.process_info['current_bw'] = processed_datasize/processed_duration/1024.0/1024
                        self.monitor_current_bw = processed_datasize/processed_duration/1024.0/1024
                        processed_datasize = 0
                        processed_duration = float(0)

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
                    time_process_start = time.time()
                    if recved_data == Const.QUEUE_SUCCESS_MESSAGE:
                        # End of the stream
                        is_end_of_stream = True
                    else:
                        required_length = 0
                        if len(memory_page_list) == 1: # handle partial data
                            last_data = memory_page_list.pop(0)
                            required_length = Memory.RAM_PAGE_SIZE-len(last_data)
                            last_data += recved_data[0:required_length]
                            memory_page_list.append(last_data)
                        memory_page_list += chunks(recved_data[required_length:], Memory.RAM_PAGE_SIZE)

            tasks = memory_page_list[0:-1]
            memory_page_list = memory_page_list[-1:]
            task_list = (ram_offset, tasks)
            (proc, task_queue, command_queue, mode_queue) = self.proc_list[proc_rr_index%self.num_proc]
            task_queue.put(task_list)

            #print "put task: offset %ld~%d at proc %d" % (ram_offset,
            #                                 ram_offset + len(tasks)*Memory.RAM_PAGE_SIZE,
            #                                 proc_rr_index%self.num_proc)
            ram_offset += (len(tasks) * Memory.RAM_PAGE_SIZE)
            proc_rr_index += 1

        # send last memory page
        task_list = (ram_offset, memory_page_list)
        (proc, task_queue, command_queue, mode_queue) = self.proc_list[proc_rr_index%self.num_proc]
        task_queue.put(task_list)

        # send end meesage to every process
        for (proc, t_queue, c_queue, mode_queue) in self.proc_list:
            t_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
            #LOG.debug("[Memory] send end message to each child")

        # after this for loop, all processing finished, but child process still
        # alive until all data pass to the next step
        finished_proc_dict = dict()
        input_list = [self.control_queue._reader.fileno()]
        for (proc, t_queue, c_queue, mode_queue) in self.proc_list:
            fileno = c_queue._reader.fileno()
            input_list.append(fileno)
            finished_proc_dict[fileno] = (c_queue, t_queue)
        while len(finished_proc_dict.keys()) > 0:
            (input_ready, [], []) = select.select(input_list, [], [])
            for in_queue in input_ready:
                if self.control_queue._reader.fileno() == in_queue:
                    control_msg = self.control_queue.get()
                    self._handle_control_msg(control_msg)
                else:
                    (cq, tq) = finished_proc_dict[in_queue]
                    cq.get()
                    del finished_proc_dict[in_queue]
        self.process_info['is_alive'] = False

        for (proc, t_queue, c_queue, mode_queue) in self.proc_list:
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


class DiffProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, mode_queue, deltalist_queue,
                 diff_algorithm, basemem_path, base_hashlist_length, 
                 memory_hashlist, free_pfn_dict, apply_free_memory):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.mode_queue = mode_queue
        self.deltalist_queue = deltalist_queue
        self.diff_algorithm = diff_algorithm
        self.basemem_path = basemem_path
        self.base_hashlist_length = base_hashlist_length
        self.memory_hashlist = memory_hashlist
        self.free_pfn_dict = free_pfn_dict
        self.apply_free_memory = apply_free_memory
        super(DiffProc, self).__init__(target=self.process_diff)

    def process_diff(self):
        self.raw_file = open(self.basemem_path, "rb")
        self.raw_mmap = mmap.mmap(self.raw_file.fileno(), 0, prot=mmap.PROT_READ)
        self.raw_filesize = os.path.getsize(self.basemem_path)

        # memory distribution measurement 
        #time_m_start = time.time()
        #out_fd = open("./memory_dist.txt", "w")

        is_proc_running = True
        input_list = [self.task_queue._reader.fileno(),
                      self.mode_queue._reader.fileno()]
        freed_page_counter = 0
        while is_proc_running:
            inready, outread, errready = select.select(input_list, [], [])
            if self.mode_queue._reader.fileno() in inready:
                # change mode
                new_mode = self.mode_queue.get()
                new_diff_algorithm = new_mode.get("diff_algorithm", None)
                sys.stdout.write("Change diff algorithm for memory from (%s) to (%s)\n" %
                                 (self.diff_algorithm, new_diff_algorithm))
                if new_diff_algorithm is not None:
                    self.diff_algorithm = new_diff_algorithm
            if self.task_queue._reader.fileno() in inready:
                task_list = self.task_queue.get()
                if task_list == Const.QUEUE_SUCCESS_MESSAGE:
                    #LOG.debug("[Memory][Child] diff proc get end message")
                    is_proc_running = False
                    break
                (start_ram_offset, memory_chunk_list) = task_list
                ram_offset = start_ram_offset
                for data in memory_chunk_list:
                    hash_list_index = ram_offset/Memory.RAM_PAGE_SIZE

                    self_hash_value = None
                    if hash_list_index < self.base_hashlist_length:
                        self_hash_value = self.memory_hashlist[hash_list_index][2]
                    chunk_hashvalue = sha256(data).digest()
                    if self_hash_value != chunk_hashvalue:
                        is_free_memory = False
                        if (self.free_pfn_dict != None) and \
                                (self.free_pfn_dict.get(long(ram_offset/Memory.RAM_PAGE_SIZE), None) == 1):
                            is_free_memory = True

                        if is_free_memory and self.apply_free_memory:
                            # Do not compare. It is free memory
                            freed_page_counter += 1
                        else:
                            try:
                                # get diff compared to the base VM
                                source_data = self.get_raw_data(ram_offset, len(data))
                                if source_data == None:
                                    msg = "launch memory snapshot is bigger than base vm at %ld (%ld > %ld)" %\
                                        (ram_offset, ram_offset+len(data), self.raw_filesize)
                                    raise IOError(msg)
                                if self.diff_algorithm == "xdelta3":
                                    diff_data = tool.diff_data(source_data, data, 2*len(source_data))
                                    diff_type = DeltaItem.REF_XDELTA
                                    if len(diff_data) > len(data):
                                        raise IOError("xdelta3 patch is bigger than origianl")
                                elif self.diff_algorithm == "none":
                                    diff_data = data
                                    diff_type = DeltaItem.REF_RAW
                                else:
                                    diff_data = data
                                    diff_type = DeltaItem.REF_RAW
                            except IOError as e:
                                diff_data = data
                                diff_type = DeltaItem.REF_RAW

                            delta_item = DeltaItem(DeltaItem.DELTA_MEMORY,
                                    ram_offset, len(data),
                                    hash_value=chunk_hashvalue,
                                    ref_id=diff_type,
                                    data_len=len(diff_data),
                                    data=diff_data)
                            self.deltalist_queue.put(delta_item)
                            #out_fd.write("%f\t%ld\n" % ((time.time()-time_m_start), ram_offset))
                    ram_offset += Memory.RAM_PAGE_SIZE
        #LOG.debug("[Memory][Child] child finished. send command queue msg")
        self.command_queue.put("processed everything")
        self.task_queue.put(freed_page_counter)
        #out_fd.close()  # measurement


    def get_raw_data(self, offset, length):
        # retrieve page data from raw memory
        if offset+length < self.raw_filesize:
            return self.raw_mmap[offset:offset+length]
        else:
            return None






'''
class FeedingPipe(multiprocessing.Process):
    def __init__(self, inpath, data_pipe):
        self.inpath = inpath 
        self.data_pipe = data_pipe
        multiprocessing.Process.__init__(self, target=self.process)

    def process(self):
        recv_pipe, send_pipe = self.data_pipe
        recv_pipe.close()
        in_fd = open(self.inpath, 'rb')
        total_bytes = 0
        while True:
            data = in_fd.read(1024*1024*1)
            total_bytes += len(data)
            send_pipe.send(data)
        LOG.debug("finish sending : %ld bytes" % total_bytes)
        in_fd.close()

def test_piping():
    memory_snapshot_path = "./memory_snap"
    (recv_data_pipe, send_data_pipe) = multiprocessing.Pipe()
    feedingProc = FeedingPipe(memory_snapshot_path, (recv_data_pipe, send_data_pipe))
    feedingProc.start()
    feedingProc.join()
    send_data_pipe.close()

'''

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

