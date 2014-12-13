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

import sys
import time
import struct
import mmap
import tool
import os
import random
import select
import threading
import traceback
import multiprocessing
import Queue
from operator import itemgetter
from hashlib import sha256
from lzma import LZMACompressor

import process_manager
from Configuration import Const
import log as logging


LOG = logging.getLogger(__name__)


class DeltaError(Exception):
    pass


class DeltaItem(object):
    # Don't change order or memory and disk
    # We're using this value to sort for self referencing
    DELTA_MEMORY        = 0x01
    DELTA_DISK          = 0x02
    DELTA_MEMORY_LIVE   = 0x03
    DELTA_DISK_LIVE     = 0x04

    REF_RAW             = 0x10
    REF_XDELTA          = 0x20
    REF_SELF            = 0x30
    REF_BASE_DISK       = 0x40
    REF_BASE_MEM        = 0x50
    REF_ZEROS           = 0x60
    REF_BSDIFF          = 0x70
    REF_SELF_HASH       = 0x80

    def __init__(self, delta_type, offset, offset_len, hash_value, ref_id,
                 data_len=0, data=None, live_seq=0):
        self.delta_type = delta_type
        self.offset = long(offset)
        self.offset_len = offset_len
        self.hash_value = hash_value
        self.ref_id = ref_id
        self.data_len = data_len
        self.data = data
        self.live_seq = live_seq

        # new field for identify delta_item
        # offset is not unique identifier since we use both memory and disk
        self.index = DeltaItem.get_index(self.delta_type, self.offset)

    @staticmethod
    def get_index(delta_type, offset):
        if delta_type == DeltaItem.DELTA_DISK or delta_type == DeltaItem.DELTA_DISK_LIVE:
            item_type = DeltaItem.DELTA_DISK
        elif delta_type == DeltaItem.DELTA_MEMORY or delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            item_type = DeltaItem.DELTA_MEMORY
        else:
            raise DeltaError("Invalid delta type: %d" % delta_type)
        return long((offset << 1) | (item_type & 0x0F))

    def __getitem__(self, item):
        return self.__dict__[item]

    def get_serialized(self, with_hashvalue=False):
        # offset        : unsigned long long
        # offset_length : unsigned short
        # ref_id        : unsigned char
        data = struct.pack("!QHc", \
                self.offset,
                self.offset_len,
                chr(self.delta_type | self.ref_id))

        if self.ref_id == DeltaItem.REF_RAW or \
                self.ref_id == DeltaItem.REF_XDELTA or \
                self.ref_id == DeltaItem.REF_BSDIFF:
            data += struct.pack("!Q", self.data_len)
            if self.data_len != 0:
                data += struct.pack("!%ds" % self.data_len, self.data)
        elif self.ref_id == DeltaItem.REF_BASE_DISK or \
                self.ref_id == DeltaItem.REF_BASE_MEM:
            data += struct.pack("!Q", self.data)
        elif self.ref_id == DeltaItem.REF_SELF:
            # saving offset for reference
            data += struct.pack("!Q", self.data)
        elif self.ref_id == DeltaItem.REF_SELF_HASH:
            # saving hashvalue for reference
            # this is for handling live migration
            data += struct.pack("!32s", self.data)

        if self.delta_type == DeltaItem.DELTA_DISK_LIVE or\
                self.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            data += struct.pack("!H", self.live_seq)

        if with_hashvalue:
            LOG.debug("hash size is %d" % len(self.hash_value))
            if self.hash_value and (len(self.hash_value) > 0):
                data += struct.pack("!%ds" % len(self.hash_value), self.hashvalue)

        return data

    @staticmethod
    def unpack_stream(stream, with_hashvalue=False):
        data = stream.read(8 + 2 + 1)
        data_len = 0
        live_seq = None
        if not data:
            return None

        (offset, offset_len, ref_info) = \
                struct.unpack("!QHc", data)
        ref_id = ord(ref_info) & 0xF0
        delta_type = ord(ref_info) & 0x0F

        if ref_id == DeltaItem.REF_RAW or \
                ref_id == DeltaItem.REF_XDELTA or \
                ref_id == DeltaItem.REF_BSDIFF:
            data_len = struct.unpack("!Q", stream.read(8))[0]
            data = stream.read(data_len)
        elif ref_id == DeltaItem.REF_BASE_DISK or \
                ref_id == DeltaItem.REF_BASE_MEM:
            data = struct.unpack("!Q", stream.read(8))[0]
        elif ref_id == DeltaItem.REF_SELF:
            data = struct.unpack("!Q", stream.read(8))[0]
        elif ref_id == DeltaItem.REF_SELF_HASH:
            #print "unpacking ref_self_hash"
            data = struct.unpack("!32s", stream.read(32))[0]

        if delta_type == DeltaItem.DELTA_DISK_LIVE or\
                delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            live_seq = struct.unpack("!H", stream.read(2))[0]

        # hash_value typically does not exist when recovered becuase we don't need it
        if with_hashvalue:
            # hash_value is only needed for residue case
            hash_value = struct.unpack("!32s", stream.read(32))[0]
            item = DeltaItem(delta_type, offset, offset_len, hash_value, ref_id, data_len, data, live_seq=live_seq)
        else:
            item = DeltaItem(delta_type, offset, offset_len, None, ref_id, data_len, data, live_seq=live_seq)
        return item


class DeltaList(object):
    @staticmethod
    def tofile(delta_list, f_path, with_hashvalue=False):
        if len(delta_list) == 0 or type(delta_list[0]) != DeltaItem:
            raise MemoryError("Need list of DeltaItem")

        fd = open(f_path, "wb")
        # Write list if delta item
        for item in delta_list:
            # save it as little endian format
            fd.write(item.get_serialized(with_hashvalue=with_hashvalue))
        fd.close()

    @staticmethod
    def fromfile(f_path):
        delta_list = []
        fd = open(f_path, "rb")
        while True:
            new_item = DeltaItem.unpack_stream(fd)
            if not new_item:
                break
            delta_list.append(new_item)
        fd.close()
        return delta_list 

    @staticmethod
    def from_stream(stream):
        while True:
            new_item = DeltaItem.unpack_stream(stream)
            if new_item == None:
                raise StopIteration()
            yield new_item

    @staticmethod
    def from_chunk(in_queue):
        while True:
            new_item = DeltaItem.unpack_stream(stream)
            if new_item == None:
                raise StopIteration()
            yield new_item

    @staticmethod
    def tofile_with_footer(footer_delta, delta_list, f_path):
        if not footer_delta:
            raise MemoryError("invalid footer delta")
        if len(delta_list) == 0 or type(delta_list[0]) != DeltaItem:
            raise MemoryError("Need list of DeltaItem")

        fd = open(f_path, "wb")

        # Write Header & Footer delta
        fd.write(struct.pack("!q", len(footer_delta)))
        fd.write(footer_delta)

        # Write list if delta item
        for item in delta_list:
            # save it as little endian format
            fd.write(item.get_serialized())
        fd.close()

    @staticmethod
    def get_self_delta(delta_list):
        if len(delta_list) == 0:
            LOG.debug("Nothing to compare. Length is 0")
            delta_list.sort(key=itemgetter('offset'))
            return
        if type(delta_list[0]) != DeltaItem:
            raise DeltaError("Need list of DeltaItem")

        # delta_list : list of (type, start, end, ref_id, hash/data)
        # sort by (hash/start offset)
        delta_list.sort(key=itemgetter('hash_value', 'delta_type', 'offset'))

        pivot = delta_list[0]
        matching = 0
        for delta_item in delta_list[1:]:
            if delta_item.hash_value == pivot.hash_value:
                if delta_item.ref_id == DeltaItem.REF_XDELTA or \
                        delta_item.ref_id == DeltaItem.REF_RAW or \
                        delta_item.ref_id == DeltaItem.REF_BSDIFF:
                    # same data/hash
                    # save reference start offset
                    delta_item.ref_id = DeltaItem.REF_SELF
                    delta_item.data_len = 8
                    delta_item.data = long(pivot.index)
                    matching += 1
                    '''
                    LOG.debug("type:%ld, offset:%ld, index:%ld <- %ld, %ld, %ld" % \
                            (pivot.delta_type, pivot.offset, pivot.index,
                            delta_item.delta_type, delta_item.offset, delta_item.index))
                    '''
            else:
                # change pivot only when no duplicated hash value
                pivot=delta_item

        LOG.debug("self delta : %ld/%ld" % (matching, len(delta_list)))

    @staticmethod
    def statistics(delta_list, mem_discarded=0, disk_discarded=0):
        if len(delta_list) == 0:
            LOG.debug("Nothing to compare. Length is 0")
            delta_list.sort(key=itemgetter('offset'))
            return
        if type(delta_list[0]) != DeltaItem:
            raise DeltaError("Need list of DeltaItem")

        memory_count = 0
        disk_count = 0
        memory_from_self = 0
        memory_from_zeros = 0
        memory_from_raw = 0
        memory_from_base_disk = 0
        memory_from_base_mem = 0
        memory_from_xdelta = 0
        memory_from_overlay_disk = 0
        memory_from_overlay_mem = 0
        disk_from_self = 0
        disk_from_zeros = 0
        disk_from_raw = 0
        disk_from_base_disk = 0
        disk_from_base_mem = 0
        disk_from_xdelta = 0
        disk_from_overlay_mem = 0
        disk_from_overlay_disk = 0
        xdelta_size = 0
        raw_size = 0

        # to quickly find memory-disk dedup
        previous_delta_dict = dict() 
        disk_overlay_size = 0
        mem_overlay_size = 0

        for delta_item in delta_list:
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                memory_count += 1
                mem_overlay_size += len(delta_item.get_serialized())
            elif delta_item.delta_type == DeltaItem.DELTA_DISK:
                disk_count += 1
                disk_overlay_size+= len(delta_item.get_serialized())

            previous_delta_dict[delta_item.index] = delta_item
            if delta_item.ref_id == DeltaItem.REF_SELF:
                ref_index = delta_item.data
                ref_delta = previous_delta_dict.get(ref_index, None)
                if ref_delta == None:
                    raise DeltaError("Cannot calculate statistics for self_referencing")
                if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                        delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                    if (ref_delta.delta_type == DeltaItem.DELTA_MEMORY) or\
                            (ref_delta.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                        disk_from_overlay_mem += 1
                    else:
                        disk_from_self += 1
                elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    (ref_delta.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                    if (ref_delta.delta_type == DeltaItem.DELTA_DISK) or\
                            (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                        memory_from_overlay_disk += 1
                    else:
                        memory_from_self += 1
            elif delta_item.ref_id == DeltaItem.REF_ZEROS:
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    memory_from_zeros += 1
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    disk_from_zeros += 1
            elif delta_item.ref_id == DeltaItem.REF_BASE_DISK:
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        (delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                    memory_from_base_disk += 1
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    disk_from_base_disk += 1
            elif delta_item.ref_id == DeltaItem.REF_BASE_MEM:
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        (delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                    memory_from_base_mem += 1
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    disk_from_base_mem += 1
            elif delta_item.ref_id == DeltaItem.REF_XDELTA or \
                delta_item.ref_id == DeltaItem.REF_BSDIFF:
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        (delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                    memory_from_xdelta += 1
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    disk_from_xdelta += 1
                xdelta_size += len(delta_item.data)
            elif delta_item.ref_id == DeltaItem.REF_RAW:
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        (delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE):
                    memory_from_raw += 1
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    (delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE):
                    disk_from_raw += 1
                raw_size += len(delta_item.data)

        total_memory_count = (memory_count + mem_discarded)
        total_disk_count = (disk_count + disk_discarded)

        try:
            LOG.info("-"*50)
            LOG.info(" Total Modified Disk #     : %ld\t( 100 %%, %f MB )" % 
                    (total_disk_count, disk_overlay_size/1024.0/1024))
            LOG.info("TRIM discard              : %ld\t( %f %% )" % 
                    (disk_discarded, disk_discarded*100.0/total_disk_count))
            LOG.info("Zero pages                : %ld\t( %f %% )" % 
                    (disk_from_zeros, disk_from_zeros*100.0/total_disk_count))
            LOG.info("Shared with Base Disk     : %ld\t( %f %% )" % 
                    (disk_from_base_disk, disk_from_base_disk*100.0/total_disk_count))
            LOG.info("Shared with Base Mem      : %ld\t( %f %% )" % 
                    (disk_from_base_mem, disk_from_base_mem*100.0/total_disk_count))
            LOG.info("Shared within Self        : %ld\t( %f %% )" % 
                    (disk_from_self, disk_from_self*100.0/total_disk_count))
            LOG.info("Shared with Overlay Disk  : %ld\t( %f %% )" % 
                    (disk_from_overlay_disk, disk_from_overlay_disk*100.0/total_disk_count))
            LOG.info("Shared with Overlay Mem   : %ld\t( %f %% )" % 
                    (disk_from_overlay_mem, disk_from_overlay_mem*100.0/total_disk_count))
            LOG.info("xdelta                    : %ld\t( %f %%, real_size: %.0f KB )" %
                    (disk_from_xdelta, disk_from_xdelta*100.0/total_disk_count, xdelta_size))
            LOG.info("raw                       : %ld\t( %f %%, real_size: %.0f KB )" % 
                    (disk_from_raw, disk_from_raw*100.0/total_disk_count, raw_size))
            LOG.info("-"*50 + "")
        except ZeroDivisionError as e:
            LOG.info("No disk modification")

        try:
            LOG.info("Total Modified Memory #  : %ld\t( 100 %%, %f MB)" % 
                    (total_memory_count, mem_overlay_size/1024.0/1024))
            LOG.info("FREE discard             : %ld\t( %f %% )" % 
                    (mem_discarded, mem_discarded*100.0/total_memory_count))
            LOG.info("Zero pages               : %ld\t( %f %% )" % 
                    (memory_from_zeros, memory_from_zeros*100.0/total_memory_count))
            LOG.info("Shared with Base Disk    : %ld\t( %f %% )" % 
                    (memory_from_base_disk, memory_from_base_disk*100.0/total_memory_count))
            LOG.info("Shared with Base Mem     : %ld\t( %f %% )" % 
                    (memory_from_base_mem, memory_from_base_mem*100.0/total_memory_count))
            LOG.info("Shared within Self       : %ld\t( %f %% )" % 
                    (memory_from_self, memory_from_self*100.0/total_memory_count))
            LOG.info("Shared with Overlay Disk : %ld\t( %f %% )" % 
                    (memory_from_overlay_disk, memory_from_overlay_disk*100.0/total_memory_count))
            LOG.info("Shared with Overlay Mem  : %ld\t( %f %% )" % 
                    (memory_from_overlay_mem, memory_from_overlay_mem*100.0/total_memory_count))
            LOG.info("xdelta                   : %ld\t( %f %%, real_size: %.0f KB )" %
                    (memory_from_xdelta, memory_from_xdelta*100.0/total_memory_count, xdelta_size))
            LOG.info("raw                      : %ld\t( %f %%, real_size: %.0f KB )" % 
                    (memory_from_raw, memory_from_raw*100.0/total_memory_count, raw_size))
            LOG.info("-"*50 + "")
        except ZeroDivisionError as e:
            LOG.info("No memory modification")


def diff_with_deltalist(delta_list, const_deltalist, ref_id):
    # update source_deltalist using const_deltalist
    # Example) source_deltalist: disk delta list,
    #       const_deltalist: memory delta list
    if len(delta_list) == 0:
        return delta_list
    if type(delta_list[0]) != DeltaItem:
        raise DeltaError("Need list of DeltaItem")
    if len(const_deltalist) == 0 or type(const_deltalist[0]) != DeltaItem:
        raise DeltaError("Need list of DeltaItem for const")

    delta_list.sort(key=itemgetter('hash_value')) # sort by hash value
    const_deltalist.sort(key=itemgetter('hash_value')) # sort by hash value

    matching_count = 0
    s_index = 0
    index = 0
    while index < len(const_deltalist) and s_index < len(delta_list):
        delta = delta_list[s_index]
        const_delta = const_deltalist[index]
        if const_delta.hash_value < delta.hash_value:
            index += 1
            continue

        # compare
        if delta.hash_value == const_delta.hash_value and \
                ((delta.ref_id == DeltaItem.REF_XDELTA) or \
                 (delta.ref_id == DeltaItem.REF_RAW) or \
                 (delta.ref_id == DeltaItem.REF_BSDIFF)):
            if delta.offset_len != const_delta.offset_len:
                message = "Hash is same but length is different %d != %d" % \
                        (delta.offset_len, const_delta.offset_len)
                raise DeltaError(message)
            matching_count += 1
            delta.ref_id = ref_id
            delta.data_len = 8
            delta.data = long(const_delta.offset)
        s_index += 1
    LOG.info("Found %d matching chunk out of %ld" % (matching_count, len(const_deltalist)))
    return delta_list 


def diff_with_hashlist(base_hashlist, delta_list, ref_id):
    # update delta_list using base_hashlist
    if len(delta_list) == 0:
        return delta_list
    if type(delta_list[0]) != DeltaItem:
        raise DeltaError("Need list of DeltaItem")

    base_hashlist.sort(key=itemgetter(2)) # sort by hash value
    delta_list.sort(key=itemgetter('hash_value')) # sort by hash value

    matching_count = 0
    s_index = 0
    index = 0
    while index < len(base_hashlist) and s_index < len(delta_list):
        delta = delta_list[s_index]
        (start, length, hash_value) = base_hashlist[index]
        if hash_value < delta.hash_value:
            index += 1
            continue

        # compare
        if delta.hash_value == hash_value and \
                ((delta.ref_id == DeltaItem.REF_XDELTA) or \
                 (delta.ref_id == DeltaItem.REF_RAW) or \
                 (delta.ref_id == DeltaItem.REF_BSDIFF)):
            matching_count += 1
            #LOG.debug("page %ld is matching base %ld" % (s_start, start))
            delta.ref_id = ref_id
            delta.data_len = 8
            delta.data = long(start)
        s_index += 1

    LOG.debug("matching (%d/%d) with base" % (matching_count, len(delta_list)))
    return delta_list


def create_overlay(memory_deltalist, memory_chunk_size,
        disk_deltalist, disk_chunk_size,
        basedisk_hashlist=None, basemem_hashlist=None):

    if memory_chunk_size != disk_chunk_size:
        raise DeltaError("Expect same chunk size for Disk and Memory")
    chunk_size = disk_chunk_size
    delta_list = memory_deltalist+disk_deltalist

    #Memory
    # Create Base Memory from meta file
    LOG.debug("2-1.Find zero page")
    zero_hash = sha256(struct.pack("!s", chr(0x00))*chunk_size).digest()
    zero_hash_list = [(-1, chunk_size, zero_hash)]
    diff_with_hashlist(zero_hash_list, delta_list, ref_id=DeltaItem.REF_ZEROS)

    LOG.debug("2-2.get delta from base Memory")
    diff_with_hashlist(basemem_hashlist, delta_list, ref_id=DeltaItem.REF_BASE_MEM)
    LOG.debug("2-3.get delta from base Disk")
    diff_with_hashlist(basedisk_hashlist, delta_list, ref_id=DeltaItem.REF_BASE_DISK)

    # 3.find shared within self
    LOG.debug("3.get delta from itself")
    DeltaList.get_self_delta(delta_list)

    return delta_list


class Recovered_delta(multiprocessing.Process):
#class Recovered_delta(threading.Thread):
    FUSE_INDEX_DISK = 1
    FUSE_INDEX_MEMORY = 2
    END_OF_PIPE = "end_of_pipe"

    def __init__(self, base_disk, base_mem, overlay_path, 
            output_mem_path, output_mem_size, 
            output_disk_path, output_disk_size, chunk_size,
            out_pipename=None, time_queue=None, deltalist_savepath=None):
        ''' recover delta list using base disk/memory
        Args:
        '''

        if base_disk == None and base_mem == None:
            raise MemoryError("Need either base_disk or base_memory")

        self.overlay_path = overlay_path
        self.output_mem_path = output_mem_path
        self.output_mem_size = output_mem_size
        self.output_disk_path = output_disk_path
        self.output_disk_size = output_disk_size
        self.out_pipename = out_pipename
        self.time_queue = time_queue
        self.base_disk = base_disk
        self.base_mem = base_mem
        self.deltalist_savepath = deltalist_savepath

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

        multiprocessing.Process.__init__(self)
        #threading.Thread.__init__(self)

    def run(self):
        start_time = time.time()

        # initialize reference data to use mmap
        count = 0
        self.base_disk_fd = open(self.base_disk, "rb")
        self.raw_disk = mmap.mmap(self.base_disk_fd.fileno(), 0, prot=mmap.PROT_READ)
        self.base_mem_fd = open(self.base_mem, "rb")
        self.raw_mem = mmap.mmap(self.base_mem_fd.fileno(), 0, prot=mmap.PROT_READ)
        self.out_pipe = open(self.out_pipename, "w")
        self.recover_mem_fd = open(self.output_mem_path, "wrb")
        self.recover_disk_fd = open(self.output_disk_path, "wrb")
        overlay_stream = open(self.overlay_path, "r")

        unresolved_deltaitem_list = []
        for delta_item in DeltaList.from_stream(overlay_stream):
            ret = self.recover_item(delta_item)
            if ret == None:
                # cannot find self reference point due to the parallel
                # compression. Save this and do it later
                unresolved_deltaitem_list.append(delta_item)
                continue
            self.process_deltaitem(delta_item)
            count += 1

        LOG.info("[Delta] Handle dangling DeltaItem (%d)" % len(unresolved_deltaitem_list))
        for delta_item in unresolved_deltaitem_list:
            ret = self.recover_item(delta_item)
            if ret == None:
                msg = "Cannot find self reference: type(%ld), offset(%ld), index(%ld)" % \
                        (delta_item.delta_type, delta_item.offset, delta_item.index)
                raise MemoryError(msg)
            self.process_deltaitem(delta_item)
            count += 1

        self.out_pipe.write(str(Recovered_delta.END_OF_PIPE) + "\n")
        self.out_pipe.close()
        self.recover_mem_fd.close()
        self.recover_mem_fd = None
        self.recover_disk_fd.close()
        self.recover_disk_fd = None
        end_time = time.time()

        if self.time_queue != None: 
            self.time_queue.put({'start_time':start_time, 'end_time':end_time})
        LOG.info("[Delta] : (%s)-(%s)=(%s), delta %ld chunks" % \
                (start_time, end_time, (end_time-start_time), count))
        self.finish()

    def recover_item(self, delta_item):
        if type(delta_item) != DeltaItem:
            raise MemoryError("Need list of DeltaItem")

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
                #raise MemoryError(msg)
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
                raise DeltaError("Delta type should be either disk or memory")
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
        else:
            raise MemoryError("Cannot recover: invalid referce id %d" % delta_item.ref_id)

        if len(recover_data) != delta_item.offset_len:
            msg = "Error, Recovered Size Error: %d, %d, ref_id: %s, data_len: %ld, offset: %ld, offset_len: %ld" % \
                    (delta_item.delta_type, len(recover_data), delta_item.ref_id, \
                    delta_item.data_len, delta_item.offset, delta_item.offset_len)
            raise MemoryError(msg)

        # recover
        delta_item.ref_id = DeltaItem.REF_RAW
        delta_item.data = recover_data
        if delta_item.hash_value == None or len(delta_item.hash_value) == 0:
            delta_item.hash_value = sha256(recover_data).digest()

        return delta_item

    def process_deltaitem(self, delta_item):
        overlay_chunk_ids = []
        if len(delta_item.data) != delta_item.offset_len:
            msg = "recovered size is not same as page size, %ld != %ld" % \
                    (len(delta_item.data), delta_item.offset_len)
            raise DeltaError(msg)

        # save it to dictionary to find self_reference easily
        self.recovered_delta_dict[delta_item.index] = delta_item
        self.recovered_hash_dict[delta_item.hash_value] = delta_item

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
        overlay_chunk_id = long(delta_item.offset/self.chunk_size)
        if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            self.recover_mem_fd.seek(delta_item.offset)
            self.recover_mem_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (Recovered_delta.FUSE_INDEX_MEMORY, overlay_chunk_id))
        elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            self.recover_disk_fd.seek(delta_item.offset)
            self.recover_disk_fd.write(delta_item.data)
            overlay_chunk_ids.append("%d:%ld" %
                    (Recovered_delta.FUSE_INDEX_DISK, overlay_chunk_id))

        # update the latest item for each memory page or disk block
        self.live_migration_iteration_dict[delta_item.index] = delta_item

        if len(overlay_chunk_ids) % 1 == 0: # to be updated
            self.recover_mem_fd.flush()
            self.recover_disk_fd.flush()

            self.out_pipe.write(",".join(overlay_chunk_ids) + '\n')
            overlay_chunk_ids[:] = []

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


def deduplicate_deltaitem(hash_dict, delta_item, ref_id):
    ref_offset = hash_dict.get(delta_item.hash_value, None)
    if ref_offset is not None:
        if ((delta_item.ref_id == DeltaItem.REF_XDELTA) or \
            (delta_item.ref_id == DeltaItem.REF_RAW) or \
            (delta_item.ref_id == DeltaItem.REF_BSDIFF)):
            delta_item.ref_id = ref_id
            delta_item.data_len = 8
            delta_item.data = ref_offset
            return True
    return False


class DeltaDedup(process_manager.ProcWorker):
    def __init__(self, memory_deltalist_queue, memory_chunk_size,
                 disk_deltalist_queue, disk_chunk_size,
                 merged_deltalist_queue,
                 overlay_creation_mode,
                 basedisk_hashdict=None, basemem_hashdict=None):
        self.memory_deltalist_queue = memory_deltalist_queue
        self.memory_chunk_size = memory_chunk_size
        self.disk_deltalist_queue = disk_deltalist_queue
        self.disk_chunk_size = disk_chunk_size
        self.merged_deltalist_queue = merged_deltalist_queue
        self.overlay_creation_mode = overlay_creation_mode
        self.basedisk_hashdict = basedisk_hashdict
        self.basemem_hashdict= basemem_hashdict

        self.self_hashdict = dict()
        self.self_hashset = set()

        # shared data with other process
        self.manager = multiprocessing.Manager()
        self.statistics = self.manager.dict()
        self.statistics['number_of_zero_page_disk'] = 0
        self.statistics['number_of_zero_page_memory'] = 0
        self.statistics['number_of_base_disk_disk'] = 0
        self.statistics['number_of_base_disk_memory'] = 0
        self.statistics['number_of_base_mem_disk'] = 0
        self.statistics['number_of_base_mem_memory'] = 0
        self.statistics['number_of_self_ref_disk'] = 0
        self.statistics['number_of_self_ref_memory'] = 0
        super(DeltaDedup, self).__init__(target=self.perform_dedup)

    def perform_dedup(self):
        total_process_time = 0
        total_process_time_block = 0
        total_ratio_block = 0
        self.in_size = 0
        self.total_block_count = 0
        self.out_size = 0

        try:
            time_start = time.time()
            is_first_recv = False
            time_first_recv = 0

            # get hashtable
            number_of_zero_page_disk = 0
            number_of_zero_page_memory = 0
            number_of_base_disk_disk = 0
            number_of_base_disk_memory = 0
            number_of_base_mem_disk = 0
            number_of_base_mem_memory = 0
            number_of_self_ref_disk = 0
            number_of_self_ref_memory = 0

            if self.memory_chunk_size != self.disk_chunk_size:
                raise DeltaError("Expect same chunk size for Disk and Memory")
            chunk_size = self.disk_chunk_size

            zero_hash_dict = dict()
            zero_hash = sha256(struct.pack("!s", chr(0x00))*chunk_size).digest()
            zero_hash_dict[zero_hash] = long(-1)
            is_memory_finished = False
            is_disk_finished = False
            while is_memory_finished == False or is_disk_finished == False:
                input_list = [self.memory_deltalist_queue._reader.fileno(),
                            self.disk_deltalist_queue._reader.fileno(),
                            self.control_queue._reader.fileno()]
                (input_ready, [], []) = select.select(input_list, [], [])
                if self.control_queue._reader.fileno() in input_ready:
                    control_msg = self.control_queue.get()
                    self._handle_control_msg(control_msg)
                for input_queue in input_ready:
                    deltaitem_list = None
                    if input_queue == self.memory_deltalist_queue._reader.fileno():
                        deltaitem_list = self.memory_deltalist_queue.get()
                        if deltaitem_list == Const.QUEUE_SUCCESS_MESSAGE:
                            is_memory_finished = True
                            continue
                    elif input_queue == self.disk_deltalist_queue._reader.fileno():
                        deltaitem_list = self.disk_deltalist_queue.get()
                        if deltaitem_list == Const.QUEUE_SUCCESS_MESSAGE:
                            is_disk_finished = True
                            continue
                    else:   # control message
                        continue

                    if is_first_recv == False:
                        is_first_recv = True
                        time_first_recv = time.time()

                    time_process_start = time.clock()
                    cur_block_count = len(deltaitem_list)
                    self.total_block_count += cur_block_count

                    indata_size_cur = 0
                    outdata_size_cur = 0
                    for delta_item in deltaitem_list:
                        indata_size_cur += (delta_item.data_len+11)
                        if deduplicate_deltaitem(zero_hash_dict, delta_item,
                                                DeltaItem.REF_ZEROS) == True:
                            if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                                number_of_zero_page_disk += 1
                            elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                                number_of_zero_page_memory += 1
                        elif deduplicate_deltaitem(self.basemem_hashdict, delta_item,
                                                DeltaItem.REF_BASE_MEM) == True:
                            if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                                number_of_base_mem_disk += 1
                            elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                                number_of_base_mem_memory += 1
                        elif deduplicate_deltaitem(self.basedisk_hashdict, delta_item,
                                                DeltaItem.REF_BASE_DISK) == True:
                            if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                                number_of_base_disk_disk += 1
                            elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                                delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                                number_of_base_disk_memory += 1
                        else:
                            # chunk that are not deduplicated yet
                            # comparison with other delta_item within itself
                            if ((delta_item.ref_id == DeltaItem.REF_XDELTA)\
                                or (delta_item.ref_id == DeltaItem.REF_RAW)\
                                or (delta_item.ref_id == DeltaItem.REF_BSDIFF)):
                                if delta_item.hash_value in self.self_hashset:
                                    delta_item.ref_id = delta_item.REF_SELF_HASH
                                    delta_item.data_len = 32
                                    delta_item.data = delta_item.hash_value
                                    if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                                            delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                                        number_of_self_ref_disk += 1
                                    elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                                        delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                                        number_of_self_ref_memory += 1
                                else:
                                    self.self_hashset.add(delta_item.hash_value)

                        # now delta item has new data length
                        outdata_size_cur += (delta_item.data_len+11)
                    self.in_size += indata_size_cur
                    self.out_size += outdata_size_cur
                    time_process_finish = time.clock()
                    self.merged_deltalist_queue.put(deltaitem_list)

                    # measurement
                    total_process_time_cur = (time_process_finish-time_process_start)
                    total_process_time += total_process_time_cur
                    if total_process_time_cur > 0:
                        self.monitor_total_time_block.value = 1000.0*total_process_time/self.total_block_count
                        self.monitor_total_ratio_block.value = (float(self.out_size)/self.in_size)

                        # for delta, processing time for most cases are zero
                        # so, current value will spike wherease average makes
                        # sense
                        self.monitor_total_time_block_cur.value = self.monitor_total_time_block.value
                        self.monitor_total_ratio_block_cur.value = self.monitor_total_ratio_block.value
                        #print "[delta] P: %f (%f/%d %f)\tR: %f (%f)" % (self.monitor_total_time_block.value, total_process_time_cur, cur_block_count, self.monitor_total_time_block_cur.value, self.monitor_total_ratio_block.value, self.monitor_total_ratio_block_cur.value)
            self.is_processing_alive.value = False
            self.finish_processing_input.value = True
            self.monitor_is_alive = False
            self.merged_deltalist_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

            self.statistics['number_of_zero_page_disk'] = number_of_zero_page_disk
            self.statistics['number_of_base_disk_disk'] = number_of_base_disk_disk
            self.statistics['number_of_base_mem_disk'] = number_of_base_mem_disk
            self.statistics['number_of_self_ref_disk'] = number_of_self_ref_disk
            self.statistics['number_of_zero_page_memory'] = number_of_zero_page_memory
            self.statistics['number_of_base_disk_memory'] = number_of_base_disk_memory
            self.statistics['number_of_base_mem_memory'] = number_of_base_mem_memory
            self.statistics['number_of_self_ref_memory'] = number_of_self_ref_memory
            saved_item = number_of_zero_page_disk + number_of_base_disk_disk + number_of_base_mem_disk +\
                number_of_self_ref_disk + number_of_zero_page_memory + number_of_base_disk_memory +\
                number_of_base_mem_memory + number_of_self_ref_memory
            time_end = time.time()

            #LOG.debug("Dedup statistics: %s" % str(self.statistics))
            LOG.debug("[time] Dedup: first input at : %f" % (time_first_recv))
            LOG.debug("profiling\t%s\tsize\t%ld\t%ld\t%f" % (self.__class__.__name__,
                                                            self.in_size,
                                                            self.out_size,
                                                            (self.out_size/float(self.in_size))))
            LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, total_process_time))
            LOG.debug("profiling\t%s\tblock-size\t%f\t%f\t%d" % (self.__class__.__name__,
                                                                float(self.in_size)/self.total_block_count,
                                                                float(self.out_size)/self.total_block_count,
                                                                self.total_block_count))
            LOG.debug("profiling\t%s\tblock-time\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, total_process_time/self.total_block_count))
        except Exception as e:
            LOG.error(str(e))
            LOG.error("failed at %s" % str(traceback.format_exc()))

    @staticmethod
    def memory_import_hashdict(meta_path):
        fd = open(meta_path, "rb")

        # Read Hash Item List
        hash_dict = dict()
        count = 0
        while True:
            count += 1
            data = fd.read(8+4+32) # start_offset, length, hash
            if not data:
                break
            item = tuple(struct.unpack("!qI32s", data))
            hash_dict[item[2]] = item[0]
        fd.close()
        return hash_dict

    @staticmethod
    def disk_import_hashdict(base_meta):
        hash_dict = dict()
        fd = open(base_meta, "rb")
        while True:
            data = fd.read(8+4+32) # start_offset, length, hash
            if not data:
                break
            item = tuple(struct.unpack("!qI32s", data))
            hash_dict[item[2]] = item[0]
        fd.close()
        return hash_dict



def reorder_deltalist_linear(chunk_size, delta_list):
    if len(delta_list) == 0 or type(delta_list[0]) != DeltaItem:
        raise MemoryError("Need list of DeltaItem")
    start_time = time.time()
    delta_dict = dict()
    for item in delta_list:
        delta_dict[item.index] = item

    delta_list.sort(key=itemgetter('delta_type', 'offset'))
    for index, delta_item in enumerate(delta_list):
        if delta_item.ref_id == DeltaItem.REF_SELF:
            ref_index = long(delta_item.data)
            ref_item = delta_dict.get(ref_index, None)
            ref_pos = delta_list.index(ref_item)
            if ref_pos > index:
                LOG.debug("REORDER] move reference from %d to %d" % (ref_pos, (index-1)))
                delta_list.remove(ref_item)
                delta_list.insert(index, ref_item)
    LOG.debug("[Debug][REORDER] reordering takes : %f" % (time.time()-start_time))


def reorder_deltalist_file(mem_access_file, chunk_size, delta_list):
    # chunks that appear earlier in access file comes afront in deltalist
    if len(delta_list) == 0 or type(delta_list[0]) != DeltaItem:
        raise MemoryError("Need list of DeltaItem")

    access_list = open(mem_access_file, "r").read().split('\n')
    if len(access_list[-1].strip()) == 0:
        access_list = access_list[:-1]
    reorder_deltalist(access_list, chunk_size, delta_list)


def reorder_deltalist_random(chunk_size, delta_list):
    access_list = [delta_item.offset/chunk_size for delta_item in delta_list]
    random.shuffle(access_list)
    reorder_deltalist(access_list, chunk_size, delta_list)


def reorder_deltalist(access_list, chunk_size, delta_list):
    start_time = time.time()
    delta_dict = dict()
    for item in delta_list:
        delta_dict[item.index] = item

    # first sort the chunks with offset
    delta_list.sort(key=itemgetter('delta_type', 'offset'))

    access_list.reverse()
    before_length = len(delta_list)
    count = 0 
    for chunk_number in access_list:
        chunk_index = DeltaItem.get_index(DeltaItem.DELTA_MEMORY, long(chunk_number)*chunk_size)
        delta_item = delta_dict.get(chunk_index, None)
        #LOG.debug("%ld in access list" % (long(chunk_number)))
        if delta_item:
            #LOG.debug("chunk(%ld) moved from %d --> 0" % (delta_item.offset/chunk_size, delta_list.index(delta_item)))
            delta_list.remove(delta_item)
            delta_list.insert(0, delta_item)
            count += 1

            # moved item has reference
            if delta_item.ref_id == DeltaItem.REF_SELF:
                ref_index = delta_item.data
                ref_delta = delta_dict[ref_index]
                delta_list.remove(ref_delta)
                delta_list.insert(0, ref_delta)
                #LOG.debug("chunk(%ld) moving because its reference of chunk(%ld)" % \
                #        (ref_delta.offset/chunk_size, delta_item.offset/chunk_size))
    after_length = len(delta_list)
    if before_length != after_length:
        raise DeltaError("DeltaList size shouldn't be changed after reordering")

    delta_dict_new = dict()
    for item in delta_list:
        delta_dict_new[item.index] = item

    prev_indexes = delta_dict.keys().sort()
    new_indexes = delta_dict_new.keys().sort()
    if prev_indexes != new_indexes:
        LOG.info("Reordered delta list is not same as previous")
        sys.exit(1)

    end_time = time.time()
    LOG.info("[DEBUG][REORDER] time %f" % (end_time-start_time))
    LOG.info("[DEBUG][REORDER] changed %d deltaitem (total access pattern: %d)" % (count, len(access_list)))



def _save_blob(start_index, delta_list, self_ref_dict, blob_name, blob_size, statistics=None):
    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
    comp_option = {'format':'xz', 'level':9}
    comp = LZMACompressor(options=comp_option)
    disk_offset_list = list()
    memory_offset_list= list()
    comp_data = ''
    original_length = 0
    index = start_index
    item_count = 0

    memory_overlay_size = 0
    disk_overlay_size = 0
    
    while index < len(delta_list):
        delta_item = delta_list[index]

        if delta_item.ref_id != DeltaItem.REF_SELF:
            # Those deduped chunks will be put right after original data
            # using deduped_list
            delta_bytes = delta_item.get_serialized()
            original_length += len(delta_bytes)
            comp_delta_bytes = comp.compress(delta_bytes)
            comp_data += comp_delta_bytes
            item_count += 1
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                    delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                memory_offset_list.append(delta_item.offset)
                memory_overlay_size += len(comp_delta_bytes)
            elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                disk_offset_list.append(delta_item.offset)
                disk_overlay_size += len(comp_delta_bytes)
            else:
                raise DeltaError("Delta should be either memory or disk")

            # remove dependece getting required index by finding reference
            deduped_list = self_ref_dict.get(delta_item.index, None)
            if deduped_list != None:
                #LOG.debug("moving %d deduped delta item" % (len(deduped_list)))
                for deduped_item in deduped_list:
                    deduped_bytes = deduped_item.get_serialized()
                    original_length += len(deduped_bytes)
                    comp_deduped_bytes = comp.compress(deduped_bytes)
                    comp_data += comp_deduped_bytes
                    item_count += 1
                    if deduped_item.delta_type == DeltaItem.DELTA_MEMORY or\
                            deduped_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                        memory_offset_list.append(deduped_item.offset)
                        memory_overlay_size += len(comp_deduped_bytes)
                    elif deduped_item.delta_type == DeltaItem.DELTA_DISK or\
                        deduped_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                        disk_offset_list.append(deduped_item.offset)
                        disk_overlay_size += len(comp_deduped_bytes)
                    else:
                        raise DeltaError("Delta should be either memory or disk")
            
        if len(comp_data) >= blob_size:
            LOG.debug("savefile for %s(%ld delta item) %ld --> %ld" % \
                    (blob_name, item_count, original_length, len(comp_data)))
            comp_data += comp.flush()
            blob_file = open(blob_name, "w+b")
            blob_file.write(comp_data)
            blob_file.close()
            if statistics != None:
                statistics['item_count'] = item_count
            return index, memory_offset_list, disk_offset_list
        index += 1

    comp_data += comp.flush()
    if len(comp_data) > 0 :
        blob_file = open(blob_name, "w+b")
        blob_file.write(comp_data)
        blob_file.close()
        if statistics != None:
            statistics['item_count'] = item_count
        return index, memory_offset_list, disk_offset_list
    else:
        raise DeltaError("LZMA compression is zero")


def divide_blobs(delta_list, overlay_path, blob_size_kb, 
        disk_chunk_size, memory_chunk_size):
    # save delta list into multiple files with LZMA compression
    start_time = time.time()

    # build reference table
    # Those deduped chunks will be put right after original data
    # using deduped_list
    self_ref_dict = dict()
    for delta_item in delta_list:
        if delta_item.ref_id == DeltaItem.REF_SELF:
            ref_index = delta_item.data
            if self_ref_dict.get(ref_index, None) == None:
                self_ref_dict[ref_index] = list()
            self_ref_dict[ref_index].append(delta_item)

    blob_size = blob_size_kb*1024
    blob_number = 1
    overlay_list = list()
    statistics = dict()
    index = 0
    comp_counter = 0
    blob_output_size = 0
    while index < len(delta_list):
        blob_name = "%s_%d.xz" % (overlay_path, blob_number)
        end_index, memory_offsets, disk_offsets = \
                _save_blob(index, delta_list, self_ref_dict, blob_name, blob_size, statistics)
        index = (end_index+1)
        blob_number += 1
        if statistics.get('item_count', None) != None:
            comp_counter += statistics.get('item_count')

        memory_chunks = [offset/memory_chunk_size for offset in memory_offsets]
        disk_chunks = [offset/disk_chunk_size for offset in disk_offsets]
        file_size = os.path.getsize(blob_name)
        blob_dict = {
            Const.META_OVERLAY_FILE_NAME:os.path.basename(blob_name),
            Const.META_OVERLAY_FILE_COMPRESSION: Const.COMPRESSION_LZMA,
            Const.META_OVERLAY_FILE_SIZE:file_size,
            Const.META_OVERLAY_FILE_DISK_CHUNKS: disk_chunks,
            Const.META_OVERLAY_FILE_MEMORY_CHUNKS: memory_chunks
        }
        overlay_list.append(blob_dict)
        blob_output_size += file_size
    end_time = time.time()
    LOG.debug("Overlay Compression time: %f, delta_item: %ld" % 
            ((end_time-start_time), comp_counter))
    LOG.debug("Total Overlay Size : %ld" % blob_output_size)
    return overlay_list


def discard_free_chunks(merged_modified_list, chunk_size, disk_discard, memory_discard):
    removing_item = list()
    if disk_discard == None:
        disk_discard = dict()
    if memory_discard == None:
        memory_discard = dict()
    
    for item in merged_modified_list:
        chunk_number = item.offset/chunk_size
        if item.delta_type == DeltaItem.DELTA_DISK or\
                item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            if disk_discard.get(chunk_number, None) != None:
                removing_item.append(item)
        if item.delta_type == DeltaItem.DELTA_MEMORY or\
                item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
            if memory_discard.get(chunk_number, None) != None:
                removing_item.append(item)

    for item in removing_item:
        merged_modified_list.remove(item)


def residue_merge_deltalist(old_deltalist, new_deltalist):
    '''return new_detlalist = old_deltalist+new_deltalist
    '''
    ret_deltalist = list()

    delta_dict = dict()
    # construct dictionary for O(1) search
    for item in old_deltalist:
        delta_dict[item.index] = item
    # construct dictionary to get SELF_REFERENCE information
    from collections import defaultdict
    reference_dict = defaultdict(list)
    for item in old_deltalist:
        if item.ref_id == DeltaItem.REF_SELF:
            original_item = delta_dict[item.data]
            reference_dict[original_item].append(item)

    for item in old_deltalist:
        ret_deltalist.append(item)

    count_new_disk = 0
    count_new_mem = 0
    count_overwrite_disk = 0
    count_overwrite_mem = 0

    for new_item in new_deltalist:
        old_item = delta_dict.get(new_item.index, None)
        if old_item == None:
            # newly generate chunk. Just append
            ret_deltalist.append(new_item)
            if new_item.delta_type == DeltaItem.DELTA_DISK or\
                    new_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                count_new_disk += 1
            elif new_item.delta_type == DeltaItem.DELTA_MEMORY or\
                new_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                count_new_mem += 1
            else:
                raise DeltaError("Invalid delta type")
        else:
            # overwrite existing one
            referred_deltalist = reference_dict.get(old_item, None)
            if referred_deltalist != None:
                #msg = "Currently RECONSTRUCT ALL SELF_REF pointer to RAW\n"
                #msg += "Windows makes kernel panic with below code"
                #raise DeltaError(msg)

                # if old_deltaitem is referenced by other deltaitem,
                # then, make the next one as a origin of reference
                new_pivot = None
                position_inlist = -1
                new_pivot_position = -1
                for position, item in enumerate(referred_deltalist):
                    try:
                        new_pivot_position = ret_deltalist.index(item)
                        new_pivot = item
                        position_inlist = position
                        break
                    except ValueError, e:
                        continue

                if new_pivot== None:
                    # all REF_SELF deltaitem is now replace
                    pass
                else:
                    ret_deltalist[new_pivot_position].ref_id = old_item.ref_id
                    ret_deltalist[new_pivot_position].data_len = old_item.data_len
                    ret_deltalist[new_pivot_position].data = old_item.data
                    ret_deltalist[new_pivot_position].hash_value = old_item.hash_value
                    del reference_dict[old_item]
                    for referred_item in referred_deltalist[position_inlist+1:]: 
                        try:
                            ref_item_index = ret_deltalist.index(referred_item)
                            ret_deltalist[ref_item_index].data = ret_deltalist[new_pivot_position].index
                            reference_dict[new_pivot].append(referred_item)
                        except ValueError, e:
                            # referred item can be already overwritten
                            pass

            # make sure to replace origin, not reference
            old_item_position = ret_deltalist.index(old_item)
            del ret_deltalist[old_item_position]
            ret_deltalist.append(new_item)

            if new_item.delta_type == DeltaItem.DELTA_DISK or\
                    new_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                count_overwrite_disk += 1
            elif new_item.delta_type == DeltaItem.DELTA_MEMORY or\
                new_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                count_overwrite_mem += 1
            else:
                raise DeltaError("Invalid delta type")

    LOG.debug("merge residue with previous :")
    LOG.debug("    add new disk   : %d" % (count_new_disk))
    LOG.debug("    add new mem    : %d" % (count_new_mem))
    LOG.debug("    overwrite disk : %d" % (count_overwrite_disk))
    LOG.debug("    overwrite mem  : %d" % (count_overwrite_mem))
    return ret_deltalist


def residue_diff_deltalists(old_deltalist, new_deltalist, base_mem):
    '''return new_detlalist = deltalist1 - deltalist2

    At this point, all delta items should be either 1) RAW of 2) XDELTA.
    If it is not, it's not possible to compare the contents of delta item.

    Args:
        base_mem : when new overlay chunks reverted back to base vm, 
            you should put the data back at new overlay. Unless, it'll
            try to use previous overaly data when merged with previous
            overlay.
    '''

    old_deltadict = dict()
    for item in old_deltalist:
        old_deltadict[item.index] = item
    new_deltadict = dict()
    for item in new_deltalist:
        new_deltadict[item.index] = item

    ret_deltalist = list()
    statics_new_item = 0
    statics_duplicated_item = 0
    statics_overwrite_item = 0
    statics_reverted = 0
    for item in new_deltalist:
        old_deltaitem = old_deltadict.get(item.index, None)
        if old_deltaitem == None:
            # newly create delta item
            ret_deltalist.append(item)
            statics_new_item += 1
        else:
            # exists at previous memory, compare them
            hash1 = old_deltaitem.hash_value
            hash2 = item.hash_value
            if hash1 == None:
                raise DeltaError("Previous delta item should have hash value")

            if hash1 != hash2:
                statics_overwrite_item += 1
                ret_deltalist.append(item)
            else:
                statics_duplicated_item += 1

    # exists at previous overlay, but not in current overlay
    # --> chunks that are converted to original
    for item in old_deltalist:
        if item.delta_type == DeltaItem.DELTA_DISK or\
                item.delta_type == DeltaItem.DELTA_DISK_LIVE:
            continue
        new_item = new_deltadict.get(item.index, None)
        if new_item != None:
            continue

        if item.offset_len != Const.CHUNK_SIZE:
            # special case: end of memory snapshot
            # memory snapshot size is not aligned with CHUNK_SIZE.
            # memory snapshot size can change every time
            base_mem_fd = open(base_mem, "r")
            base_mem_fd.seek(item.offset)
            base_mem_data = base_mem_fd.read(Const.CHUNK_SIZE)
            base_mem_hash = sha256(base_mem_data).digest()
            data_len = len(base_mem_data)
            if len(base_mem_data) == Const.CHUNK_SIZE:
                msg = "Error, This is not possible.\n\
                        This should be the end of memory snapshot"
                raise DeltaError(msg)

            delta_item = DeltaItem(item.delta_type, item.offset, data_len,
                    hash_value=base_mem_hash, ref_id=DeltaItem.REF_RAW,
                    data_len=data_len, data=base_mem_data)
        else:
            delta_item = DeltaItem(item.delta_type, item.offset, item.offset_len,
                    hash_value=None, ref_id=DeltaItem.REF_BASE_MEM,
                    data_len=8, data=item.offset)
        ret_deltalist.append(delta_item)
        statics_reverted += 1

    LOG.debug("residue_diff_statistics")
    LOG.debug("  newly create chunks   : %d" % (statics_new_item))
    LOG.debug("  overwrite to previous : %d" % (statics_overwrite_item))
    LOG.debug("  identical to previous : %d" % (statics_duplicated_item))
    LOG.debug("  reverted back         : %d" % (statics_reverted))

    return ret_deltalist
