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

import struct
import os
import sys
import time
import mmap
import ctypes
import multiprocessing
import select
import Queue
import traceback
from math import ceil
from hashlib import sha256
from operator import itemgetter

import tool
import delta
import process_manager
from delta import DeltaItem
from delta import DeltaList
from delta import Recovered_delta
from progressbar import AnimatedProgressBar
from Configuration import Const
from Configuration import VMOverlayCreationMode
import log as logging

LOG = logging.getLogger(__name__)


class DiskError(Exception):
    pass


def hashing(disk_path, meta_path, chunk_size=4096, window_size=512):
    # TODO: need more efficient implementation, e.g. bisect
    # generate hash of base disk
    # disk_path : raw disk path
    # chunk_size : hash chunk size
    # window_size : slicing window size

    prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)
    total_iteration = os.path.getsize(disk_path)/window_size
    iter_count = 0
    prog_interval = 100

    disk_file = open(disk_path, "rb")
    out_file = open(meta_path, "w+b")
    data = disk_file.read(chunk_size)
    if (not data) or len(data) < chunk_size:
        raise DiskError("invalid raw disk size")

    entire_hashing = sha256()
    entire_hashing.update(data)

    s_offset = 0
    data_len = len(data)
    hash_dic = dict()
    while True:
        if (iter_count)%prog_interval == 0:
            prog_bar.process(100.0*prog_interval/total_iteration)
            prog_bar.show_progress()
        iter_count += 1

        hashed_data = sha256(data).digest()
        if hash_dic.get(hashed_data) == None:
            hash_dic[hashed_data]= (hashed_data, s_offset, data_len)

        added_data = disk_file.read(window_size)
        if (not added_data) or len(added_data) != window_size:
            break
        s_offset += window_size
        data = data[window_size:] + added_data
        entire_hashing.update(added_data)

    for hashed_data, s_offset, data_len in list(hash_dic.values()):
        out_file.write(struct.pack("!QI%ds" % len(hashed_data), 
            s_offset, data_len, hashed_data))
    disk_file.close()
    out_file.close()

    return entire_hashing.hexdigest()


def _pack_hashlist(hash_list):
    # pack hash list
    original_length = len(hash_list)
    hash_list = dict((x[0], x) for x in hash_list).values()
    LOG.info("hashlist is packed: from %d to %d : %lf" % \
            (original_length, len(hash_list), 1.0*len(hash_list)/original_length))


def parse_qemu_log(qemu_logfile, chunk_size):
    # return dma_dict, discard_dict
    # element of dictionary has (chunk_%:discarded_time) format
    # CAVEAT: DMA Memory Address should be sift 4096*2 bytes because 
    # of libvirt(4096) and KVM(4096) header offset
    MEM_SIFT_OFFSET = 4096+4096
    if (qemu_logfile == None) or (not os.path.exists(qemu_logfile)):
        return dict(), dict()

    discard_dict = dict()
    dma_dict = dict()
    lines = open(qemu_logfile, "r").read().split("\n")
    discard_counter = 0
    dma_counter = 0
    mal_aligned_sector = 0
    total_founded_discard = 0
    effective_discard = 0
    for line in lines[:-1]: # last line might not be perfect
        if not line:
            break
        splits = line.split(",")
        event_time = float(splits[0].strip().split(":")[-1])
        header = splits[1].strip()
        data = splits[2:]
        if header == 'dma':
            mem_addr = long(data[0].split(":")[-1])
            sec_num = long(data[1].split(":")[-1])
            sec_len = long(data[2].split(":")[-1])
            from_disk = long(data[3].split(":")[-1])
            mem_chunk = (mem_addr+MEM_SIFT_OFFSET)/chunk_size
            disk_chunk = sec_num*512.0/chunk_size
            if sec_len != chunk_size:
                msg = "DMA sector length(%d) is not same as chunk size(%d)" % (sec_len, chunk_size)
                raise DiskError(msg)
            if sec_num%8 == 0:
                dma_dict[disk_chunk] = {'time':event_time, 'mem_chunk':mem_chunk, 'read':(True if from_disk else False)}
                dma_counter += 1
            else:
                if sec_num != -1:
                    pass
        elif header == 'bdrv_discard':
            start_sec_num = long(data[0].split(":")[-1])
            total_sec_len = long(data[1].split(":")[-1])
            start_chunk_num = start_sec_num*512.0/chunk_size
            end_chunk_num = (start_sec_num*512 + total_sec_len*512)/chunk_size
            if (start_sec_num*512)%chunk_size != 0:
                mal_aligned_sector += total_sec_len
                #LOG.warning("Warning, disk sector is not aligned with chunksize")
            total_founded_discard += (total_sec_len*512)

            start_chunk_num = int(ceil(start_chunk_num))
            for chunk_num in xrange(start_chunk_num, end_chunk_num):
                discard_dict[chunk_num] = event_time
                discard_counter += 1

    if mal_aligned_sector != 0:
        LOG.warning("Lost %d bytes from mal-alignment" % (mal_aligned_sector*512))
    if total_founded_discard != 0:
        LOG.debug("Total founded TRIM: %d B, effective TRIM: %d B" % \
                (total_founded_discard, len(discard_dict)*chunk_size))
    if dma_counter != 0 :
        LOG.debug("net DMA ratio : %ld/%ld = %f %%" % \
                (len(dma_dict), dma_counter, 100.0*len(dma_dict)/dma_counter))
    if discard_counter != 0:
        LOG.debug("net discard ratio : %ld/%ld = %f %%" % \
                (len(discard_dict), discard_counter, 100.0*len(discard_dict)/discard_counter))
    if mal_aligned_sector != 0:
        LOG.warning("Warning, mal-alignedsector count: %d" % (mal_aligned_sector))
    return dma_dict, discard_dict


class CreateDiskDeltalist(process_manager.ProcWorker):
    def __init__(self, modified_disk, 
                 modified_chunk_queue, chunk_size,
                 disk_deltalist_queue,
                 basedisk_path,
                 overlay_mode,
                 trim_dict=None, dma_dict=None,
                 apply_discard=True,
                 used_blocks_dict=None):
        # get disk delta
        # base_diskmeta : hash list of base disk
        # base_disk: path to base VM disk
        # modified_disk_path : path to modified VM disk
        # modified_chunk_queue : chunk dict of modified
        # overlay_path : path to destination of overlay disk
        # dma_dict : dma information, 
        #           dma_dict[disk_chunk] = {'time':time, 'memory_chunk':memory chunk number, 'read': True if read from disk'}
        self.modified_disk = modified_disk
        self.modified_chunk_queue = modified_chunk_queue
        self.chunk_size = chunk_size
        self.disk_deltalist_queue = disk_deltalist_queue
        self.basedisk_path = basedisk_path
        self.trim_dict = trim_dict
        self.dma_dict = dma_dict
        self.apply_discard = apply_discard
        self.used_blocks_dict = used_blocks_dict
        self.proc_list = list()
        self.overlay_mode = overlay_mode
        self.num_proc = VMOverlayCreationMode.MAX_CPU_CORE
        self.diff_algorithm = overlay_mode.DISK_DIFF_ALGORITHM

        self.manager = multiprocessing.Manager()
        self.ret_statistics = self.manager.dict()
        super(CreateDiskDeltalist, self).__init__(target=self.create_disk_deltalist)

    def change_mode(self, new_mode):
        for (proc, c_queue, m_queue) in self.proc_list:
            if proc.is_alive() == True:
                m_queue.put(new_mode)

    def create_disk_deltalist(self):
        time_start = time.time()
        self.total_block = 0
        self.total_time = float(0)
        is_first_recv = False
        time_first_recv = 0
        time_process_start = 0
        time_prev_report = 0
        UPDATE_PERIOD = self.process_info['update_period']

        # 0. get info from qemu log file
        # dictionary : (chunk_%, discarded_time)
        trim_counter = 0
        overwritten_after_trim = 0
        xray_counter = 0

        # TO BE DELETED
        trimed_list = []
        xrayed_list = []

        # launch child processes
        task_queue = multiprocessing.Queue(maxsize=VMOverlayCreationMode.MAX_CPU_CORE)
        for i in range(self.num_proc):
            command_queue = multiprocessing.Queue()
            mode_queue = multiprocessing.Queue()
            diff_proc = DiskDiffProc(command_queue, task_queue, mode_queue,
                                     self.disk_deltalist_queue,
                                     self.diff_algorithm,
                                     self.basedisk_path,
                                     self.modified_disk,
                                     self.chunk_size)
            diff_proc.start()
            self.proc_list.append((diff_proc, command_queue, mode_queue))

        # 1. get modified page
        try:
            modified_chunk_list = []
            input_fd = [self.modified_chunk_queue._reader.fileno(), self.control_queue._reader.fileno()]
            while True:
                input_ready, out_ready, err_ready = select.select(input_fd, [], [])
                if self.control_queue._reader.fileno() in input_ready:
                    control_msg = self.control_queue.get()
                    ret = self._handle_control_msg(control_msg)
                    if ret == False:
                        if control_msg == "change_mode":
                            new_mode = self.control_queue.get()
                            self.change_mode(new_mode)
                if self.modified_chunk_queue._reader.fileno() in input_ready:
                    recv_data = self.modified_chunk_queue.get()
                    if recv_data == Const.QUEUE_SUCCESS_MESSAGE:
                        break
                    time_process_start = time.time()
                    (chunk, ctime) = recv_data
                    offset = chunk * self.chunk_size

                    # check TRIM discard
                    is_discarded = False
                    if self.trim_dict:
                        trim_time = self.trim_dict.get(chunk, None)
                        if trim_time:
                            if (trim_time > ctime):
                                trimed_list.append(chunk)
                                trim_counter += 1
                                is_discarded = True
                            else:
                                overwritten_after_trim += 1
                    if is_discarded == True:
                        # only apply when it is true
                        if self.apply_discard:
                            continue

                    modified_chunk_list.append(chunk)
                    if is_first_recv == False:
                        is_first_recv = True
                        time_first_recv = time.time()

                    modified_chunk_length = len(modified_chunk_list)
                    if modified_chunk_length > 255:  # 1MB
                        task_queue.put(modified_chunk_list)
                        modified_chunk_list = []

                        total_process_time_block = 0
                        total_ratio_block = 0
                        total_process_time_block_cur = 0
                        total_ratio_block_cur = 0
                        total_input_size = 0
                        total_output_size = 0
                        valid_child_proc = 0
                        for (proc, c_queue, mode_queue) in self.proc_list:
                            process_time_block = proc.child_process_time_block.value
                            ratio_block = proc.child_ratio_block.value
                            process_time_block_cur = proc.child_process_time_block_cur.value
                            ratio_block_cur = proc.child_ratio_block_cur.value
                            input_size = proc.child_input_size.value
                            output_size = proc.child_output_size.value
                            if (process_time_block > 0) and (ratio_block > 0):
                                valid_child_proc += 1
                                total_process_time_block += process_time_block
                                total_ratio_block += ratio_block
                                total_process_time_block_cur += process_time_block_cur
                                total_ratio_block_cur += ratio_block_cur
                                total_input_size += input_size
                                total_output_size += output_size
                        if valid_child_proc > 0:
                            self.monitor_total_time_block.value = total_process_time_block/valid_child_proc
                            self.monitor_total_ratio_block.value = total_ratio_block/valid_child_proc
                            self.monitor_total_time_block_cur.value = total_process_time_block_cur/valid_child_proc
                            self.monitor_total_ratio_block_cur.value = total_ratio_block_cur/valid_child_proc
                            #print "[disk] P: %f (%f)\tR: %f (%f)" % (self.monitor_total_time_block.value, self.monitor_total_time_block_cur.value, self.monitor_total_ratio_block.value, self.monitor_total_ratio_block_cur.value)
                        self.monitor_total_input_size.value = total_input_size
                        self.monitor_total_output_size.value = total_output_size
                        #print "[disk] total input size: %d, total_output size: %d" % (self.monitor_total_input_size.value, self.monitor_total_output_size.value)

            self.process_info['finish_processing_input'] = True

            # send last chunks
            if len(modified_chunk_list) > 0:
                task_queue.put(modified_chunk_list)
                modified_chunk_list = []

            # send end meesage to every process
            for index in self.proc_list:
                task_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
                #LOG.debug("[Disk] send end message to each child")

            # after this for loop, all processing finished, but child process still
            # alive until all data pass to the next step
            finished_proc_dict = dict()
            input_list = [self.control_queue._reader.fileno()]
            for (proc, c_queue, m_queue) in self.proc_list:
                fileno = c_queue._reader.fileno()
                input_list.append(fileno)
                finished_proc_dict[fileno] = c_queue
            while len(finished_proc_dict.keys()) > 0:
                #print "disk left proc number: %s" % (finished_proc_dict.keys())
                #for ffno, (cq, tq) in finished_proc_dict.iteritems():
                #    print "task quesize at %d: %d" % (ffno, tq.qsize())
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
            self.process_info['is_alive'] = False
        except Exception as e:
            print str(e)
            LOG.error("failed at %s" % str(traceback.format_exc()))


        for (proc, c_queue, m_queue) in self.proc_list:
            #LOG.debug("[Disk] waiting to dump all data to the next stage")
            proc.join()
        # send end message after the next stage finishes processing
        self.disk_deltalist_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        if self.ret_statistics != None:
            self.ret_statistics['trimed'] = trim_counter
            self.ret_statistics['xrayed'] = xray_counter
            self.ret_statistics['trimed_list'] = trimed_list
            self.ret_statistics['xrayed_list'] = xrayed_list
        #LOG.debug("1-1. Trim(%d, overwritten after trim(%d)), Xray(%d)" % \
        #        (trim_counter, overwritten_after_trim, xray_counter))
        time_end = time.time()
        LOG.debug("[time] Disk first input at : %f" % (time_first_recv))
        if self.out_size != 0:
            in_out_ratio = self.out_size/float(self.in_size)
        else:
            in_out_ratio = 1
        LOG.debug("profiling\t%s\tsize\t%ld\t%ld\t%f" % (self.__class__.__name__,
                                                        self.in_size,
                                                        self.out_size,
                                                        in_out_ratio))
        LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f" %\
                  (self.__class__.__name__, time_start, time_end, self.total_time))
        if self.total_block > 0:
            LOG.debug("profiling\t%s\tblock-size\t%f\t%f\t%d" % (self.__class__.__name__,
                                                                float(self.in_size)/self.total_block,
                                                                float(self.out_size)/self.total_block,
                                                                self.total_block))
            LOG.debug("profiling\t%s\tblock-time\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, self.total_time/self.total_block))
        else:
            LOG.debug("profiling\t%s\tblock-size\t%f\t%f\t%d" % (self.__class__.__name__,
                                                                 0,
                                                                 0,
                                                                 self.total_block))
            LOG.debug("profiling\t%s\tblock-time\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, (time_end-time_start)/1))


def recover_disk(base_disk, base_mem, overlay_mem, overlay_disk, recover_path, chunk_size):
    recover_fd = open(recover_path, "wb")

    # get delta list from file and recover it to origin
    delta_stream = open(overlay_disk, "r")
    recovered_memory = Recovered_delta(base_disk, base_mem, chunk_size, \
            parent=base_disk, overlay_memory=overlay_mem)
    for delta_item in DeltaList.from_stream(delta_stream):
        recovered_memory.recover_item(delta_item)
    delta_list = recovered_memory.delta_list

    # overlay map
    chunk_list = []
    # sort delta list using offset
    delta_list.sort(key=itemgetter('offset'))
    for delta_item in delta_list:
        if len(delta_item.data) != chunk_size:
            raise DiskError("recovered size is not same as page size")
        chunk_list.append("%ld:1" % (delta_item.offset/chunk_size))
        recover_fd.seek(delta_item.offset)
        recover_fd.write(delta_item.data)
        last_write_offset = delta_item.offset + len(delta_item.data)

    # fill zero to the end of the modified file
    if last_write_offset:
        diff_offset = os.path.getsize(base_disk) - last_write_offset
        if diff_offset > 0:
            recover_fd.seek(diff_offset-1, os.SEEK_CUR)
            recover_fd.write('0')
    recover_fd.close()

    # overlay chunk format: chunk_1:1,chunk_2:1,...
    return ','.join(chunk_list)


class DiskDiffProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, mode_queue, deltalist_queue,
                 diff_algorithm, basedisk_path, modified_disk, chunk_size):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.mode_queue = mode_queue
        self.deltalist_queue = deltalist_queue
        self.diff_algorithm = diff_algorithm
        self.basedisk_path = basedisk_path
        self.modified_disk = modified_disk
        self.chunk_size = chunk_size
        self.measure_history = list()

        # shared variables between processes
        self.child_process_time_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_ratio_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_process_time_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_ratio_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_input_size = multiprocessing.RawValue(ctypes.c_ulong, 0)
        self.child_output_size = multiprocessing.RawValue(ctypes.c_ulong, 0)

        super(DiskDiffProc, self).__init__(target=self.process_diff)

    def process_diff(self):
        base_fd = open(self.basedisk_path, "rb")
        base_mmap = mmap.mmap(base_fd.fileno(), 0, prot=mmap.PROT_READ)
        modified_fd = open(self.modified_disk, "rb")

        time_process_total_time = float(0)
        child_total_block = 0
        indata_size = 0
        outdata_size = 0

        is_proc_running = True
        input_list = [self.task_queue._reader.fileno(),
                      self.mode_queue._reader.fileno()]
        while is_proc_running:
            inready, outread, errready = select.select(input_list, [], [])
            if self.mode_queue._reader.fileno() in inready:
                # change mode
                (command, value) = self.mode_queue.get()
                if command == "new_mode":
                    new_mode = value
                    new_diff_algorithm = new_mode.get("diff_algorithm", None)
                    sys.stdout.write("Change diff algorithm for disk from (%s) to (%s)\n" %
                                    (self.diff_algorithm, new_diff_algorithm))
                    if new_diff_algorithm is not None:
                        self.diff_algorithm = new_diff_algorithm
                elif command == "new_num_cores":
                    new_num_cores = value
                    #print "[disk] child receives new num cores: %s" % (new_num_cores)
                    if new_num_cores is not None:
                        VMOverlayCreationMode.set_num_cores(new_num_cores)
            if self.task_queue._reader.fileno() in inready:
                task_list = self.task_queue.get()
                if task_list == Const.QUEUE_SUCCESS_MESSAGE:
                    #LOG.debug("[Disk][Child] diff proc get end message")
                    is_proc_running = False
                    break

                time_process_start = time.clock()
                deltaitem_list = list()
                child_cur_block_count = 0
                indata_size_cur = 0
                outdata_size_cur = 0
                for chunk in task_list:
                    offset = chunk * self.chunk_size
                    # check file system 
                    modified_fd.seek(offset)
                    data = modified_fd.read(self.chunk_size)
                    chunk_data_len = len(data)
                    source_data = base_mmap[offset:offset+chunk_data_len]
                    try:
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
                    delta_item = DeltaItem(DeltaItem.DELTA_DISK,
                            offset, len(data),
                            hash_value=sha256(data).digest(),
                            ref_id=diff_type,
                            data_len=diff_data_len,
                            data=diff_data)
                    deltaitem_list.append(delta_item)
                time_process_end = time.clock()
                child_total_block += child_cur_block_count
                time_process_cur_time = (time_process_end - time_process_start)
                time_process_total_time += time_process_cur_time
                indata_size += indata_size_cur
                outdata_size += outdata_size_cur
                self.child_input_size.value = indata_size
                self.child_output_size.value = outdata_size
                if child_cur_block_count > 0:
                    self.child_process_time_block.value = 1000.0*time_process_total_time/child_total_block
                    self.child_ratio_block.value = outdata_size/float(indata_size)

                    cur_p = 1000.0*time_process_cur_time/child_cur_block_count
                    cur_r = outdata_size_cur/float(indata_size_cur)
                    self.measure_history.append((time_process_end, cur_p, cur_r))
                    cur_p_avg, cur_r_avg = self.averaged_value(time_process_end)
                    self.child_process_time_block_cur.value = cur_p_avg
                    self.child_ratio_block_cur.value = cur_r_avg

                self.deltalist_queue.put(deltaitem_list)
        LOG.debug("[Disk][Child] Child finished. process %d jobs (%f)" % (child_total_block, time_process_total_time))
        self.child_process_time_block.value = 0
        self.child_ratio_block.value = 0
        self.child_process_time_block_cur.value = 0
        self.child_ratio_block_cur.value = 0
        self.command_queue.put((indata_size, outdata_size, child_total_block, time_process_total_time))
        while self.mode_queue.empty() == False:
            self.mode_queue.get_nowait()
            msg = "Empty new compression mode that does not refelected"
            sys.stdout.write(msg)

    def averaged_value(self, cur_time):
        avg_p = float(0)
        avg_r = float(0)
        counter = 0
        for (measured_time, p, r) in reversed(self.measure_history):
            if cur_time - measured_time > VMOverlayCreationMode.MEASURE_AVERAGE_TIME:
                break
            avg_p += p
            avg_r += r
            counter += 1
        self.measure_history = self.measure_history[-1*counter:]
        return avg_p/counter, avg_r/counter


if __name__ == "__main__":
    parse_qemu_log("log", 4096)
