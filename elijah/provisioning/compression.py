#!/usr/bin/env python
import os
import time
import sys
import select
import subprocess
import threading
import msgpack
import multiprocessing
import traceback
import ctypes

from delta import DeltaItem

import lzma
import bz2
import zlib
from Configuration import Const
from package import VMOverlayPackage
import process_manager
import log as logging

LOG = logging.getLogger(__name__)


class CompressionError(Exception):
    pass



class CompressProc(process_manager.ProcWorker):
    def __init__(self, delta_list_queue, comp_delta_queue,
                 overlay_mode,
                 block_size=1024*1024*2):
        """
        comparisons of compression algorithm
        http://pokecraft.first-world.info/wiki/Quick_Benchmark:_Gzip_vs_Bzip2_vs_LZMA_vs_XZ_vs_LZ4_vs_LZO
        """
        self.delta_list_queue = delta_list_queue
        self.comp_delta_queue = comp_delta_queue
        self.overlay_mode = overlay_mode
        self.comp_type = overlay_mode.COMPRESSION_ALGORITHM_TYPE
        self.num_proc = overlay_mode.NUM_PROC_COMPRESSION
        self.comp_level = overlay_mode.COMPRESSION_ALGORITHM_SPEED
        self.block_size = block_size
        self.proc_list = list()

        super(CompressProc, self).__init__(target=self.compress_stream)

    def change_mode(self, new_mode):
        for (proc, c_queue, m_queue) in self.proc_list:
            if proc.is_alive() == True:
                m_queue.put(new_mode)

    def _chunk_blob(self):
        input_size = 0
        input_deltalist = list()
        is_last_blob = False
        input_list = [self.control_queue._reader.fileno(),
                        self.delta_list_queue._reader.fileno()]
        while input_size < self.block_size:
            (input_ready, [], []) = select.select(input_list, [], [], 0.01)
            if self.control_queue._reader.fileno() in input_ready:
                control_msg = self.control_queue.get()
                ret = self._handle_control_msg(control_msg)
                if ret == False:
                    if control_msg == "change_mode":
                        new_mode = self.control_queue.get()
                        self.change_mode(new_mode)
            if self.delta_list_queue._reader.fileno() in input_ready:
                deltaitem_list = self.delta_list_queue.get()
                if self.is_first_recv == False:
                    self.is_first_recv = True
                    self.time_first_recv = time.time()
                    LOG.debug("[time] Compression first input at : %f" % (self.time_first_recv))
                if deltaitem_list == Const.QUEUE_SUCCESS_MESSAGE:
                    is_last_blob = True
                    break
                recved_size = 0
                for delta_item in deltaitem_list:
                    recved_size += (delta_item.data_len + 11 + 8)
                input_size += recved_size
                input_deltalist += deltaitem_list
        return is_last_blob, input_deltalist

    def compress_stream(self):
        self.total_block = 0
        self.total_time = 0
        self.is_first_recv = False
        self.time_first_recv = 0
        try:
            time_start = time.time()

            # launch child processes
            self.task_queue = multiprocessing.Queue(maxsize=self.overlay_mode.NUM_PROC_COMPRESSION)
            for i in range(self.num_proc):
                command_queue = multiprocessing.Queue()
                mode_queue = multiprocessing.Queue()
                comp_proc = CompChildProc(command_queue, self.task_queue, mode_queue,
                                        self.comp_delta_queue,
                                        self.comp_type,
                                        self.comp_level)
                comp_proc.start()
                self.proc_list.append((comp_proc, command_queue, mode_queue))

            is_last_blob = False
            while is_last_blob == False:
                # read data
                is_last_blob, input_deltalist = self._chunk_blob()

                if len(input_deltalist) > 0:
                    self.task_queue.put(input_deltalist)
                    # measurement
                    total_process_time_block = 0
                    total_ratio_block = 0
                    total_process_time_block_cur = 0
                    total_ratio_block_cur = 0
                    valid_child_proc = 0
                    for (proc, c_queue, mode_queue) in self.proc_list:
                        process_time_block = proc.child_process_time_block.value
                        ratio_block = proc.child_ratio_block.value
                        process_time_block_cur = proc.child_process_time_block_cur.value
                        ratio_block_cur = proc.child_ratio_block_cur.value
                        if (process_time_block > 0) and (ratio_block > 0):
                            valid_child_proc += 1
                            total_process_time_block += process_time_block
                            total_ratio_block += ratio_block
                            total_process_time_block_cur += process_time_block_cur
                            total_ratio_block_cur += ratio_block_cur
                        #sys.stdout.write("(%f)\t" % (ratio_block))
                    #print "%d" % valid_child_proc
                    if valid_child_proc > 0:
                        self.monitor_total_time_block.value = total_process_time_block/valid_child_proc
                        self.monitor_total_ratio_block.value = total_ratio_block/valid_child_proc
                        self.monitor_total_time_block_cur.value = total_process_time_block_cur/valid_child_proc
                        self.monitor_total_ratio_block_cur.value = total_ratio_block_cur/valid_child_proc
                        print "[comp] P: %f (%f)\tR: %f (%f)" % (self.monitor_total_time_block.value, self.monitor_total_time_block_cur.value, self.monitor_total_ratio_block.value, self.monitor_total_ratio_block_cur.value)

            # send end meesage to every process
            for index in self.proc_list:
                self.task_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
                #sys.stdout.write("[Comp] send end message to each child\n")

            # after this for loop, all processing finished, but child process still
            # alive until all data pass to the next step
            finished_proc_dict = dict()
            input_list = [self.control_queue._reader.fileno()]
            for (proc, c_queue, m_queue) in self.proc_list:
                fileno = c_queue._reader.fileno()
                input_list.append(fileno)
                finished_proc_dict[fileno] = c_queue
            while len(finished_proc_dict.keys()) > 0:
                #print "left proc number: %s" % (finished_proc_dict.keys())
                #for ffno, cq in finished_proc_dict.iteritems():
                #    print "task quesize at %d: %d" % (ffno, task_queue.qsize())

                (input_ready, [], []) = select.select(input_list, [], [])
                for in_queue in input_ready:
                    if self.control_queue._reader.fileno() == in_queue:
                        control_msg = self.control_queue.get()
                        ret = self._handle_control_msg(control_msg)
                        if ret == False:
                            if control_msg == "change_mode":
                                new_mode = self.control_queue.get()
                                self.change_mode(new_mode)
                    else:
                        cq = finished_proc_dict[in_queue]
                        (input_size, output_size, blocks, processed_time) = cq.get()
                        self.in_size += input_size
                        self.out_size += output_size
                        self.total_block += blocks
                        self.total_time += processed_time
                        del finished_proc_dict[in_queue]
            self.process_info['is_alive'] = False

            time_end = time.time()
            #sys.stdout.write("[Comp] effetively finished\n")
            LOG.debug("profiling\t%s\tsize\t%ld\t%ld\t%f" % (self.__class__.__name__,
                                                            self.in_size,
                                                            self.out_size,
                                                            (self.out_size/float(self.in_size))))
            LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, self.total_time))
            LOG.debug("profiling\t%s\tblock-size\t%f\t%f\t%d" % (self.__class__.__name__,
                                                                float(self.in_size)/self.total_block,
                                                                float(self.out_size)/self.total_block,
                                                                self.total_block))
            LOG.debug("profiling\t%s\tblock-time\t%f\t%f\t%f" %\
                    (self.__class__.__name__, time_start, time_end, self.total_time/self.total_block))

            for (proc, c_queue, m_queue) in self.proc_list:
                #sys.stdout.write("[Comp] waiting to dump all data to the next stage\n")
                proc.join()
            # send end message after the next stage finishes processing
            self.comp_delta_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.write(traceback.format_exc())



class CompChildProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, mode_queue,
                 output_queue, comp_type, comp_level):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.mode_queue = mode_queue
        self.output_queue = output_queue
        self.comp_type = comp_type
        self.comp_level = comp_level

        # shared variables between processes
        self.child_process_time_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_ratio_block = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_process_time_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)
        self.child_ratio_block_cur = multiprocessing.RawValue(ctypes.c_double, 0)

        super(CompChildProc, self).__init__(target=self._comp)

    def _comp(self):
        is_proc_running = True
        input_list = [self.task_queue._reader.fileno(),
                      self.mode_queue._reader.fileno()]
        indata_size = 0
        outdata_size = 0
        child_total_block = 0
        time_process_total_time = 0
        loop_counter = 0
        while is_proc_running:
            inready, outread, errready = select.select(input_list, [], [])
            if self.mode_queue._reader.fileno() in inready:
                # change mode
                new_mode = self.mode_queue.get()
                new_comp_type = new_mode.get("comp_type", None)
                new_comp_level = new_mode.get("comp_level", None)
                sys.stdout.write("Change Compression mode: from (%s, %s) to (%s, %s)\n" %
                                 (self.comp_type, self.comp_level,
                                  new_comp_type, new_comp_level))
                if new_comp_type is not None:
                    self.comp_type = new_comp_type
                if new_comp_level is not None:
                    self.comp_level = new_comp_level
            if self.task_queue._reader.fileno() in inready:
                input_task = self.task_queue.get()
                if input_task == Const.QUEUE_SUCCESS_MESSAGE:
                    #sys.stdout.write("[Comp][Child] LZMA proc get end message\n")
                    is_proc_running = False
                    break
                deltaitem_list = input_task
                comp_type_cur = self.comp_type
                #print "compression: %d %d" % (self.comp_type, self.comp_level)
                loop_counter += 1

                # get compressor
                if comp_type_cur == Const.COMPRESSION_LZMA:
                    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
                    comp = lzma.LZMACompressor(options={'format':'xz', 'level':self.comp_level})
                elif comp_type_cur == Const.COMPRESSION_BZIP2:
                    comp = bz2.BZ2Compressor(self.comp_level)
                elif comp_type_cur == Const.COMPRESSION_GZIP:
                    comp = zlib.compressobj(self.comp_level, zlib.DEFLATED, zlib.MAX_WBITS | 16)
                else:
                    raise CompressionError("Not supporting")

                # compression for each block
                modified_memory_chunks = list()
                modified_disk_chunks = list()
                output_data = ''
                input_data = ''
                child_cur_block_count = 0
                indata_size_cur = 0
                outdata_size_cur = 0
                time_process_cur_time = 0

                time_process_start = time.clock()
                for delta_item in deltaitem_list:
                    delta_bytes = delta_item.get_serialized()
                    offset = delta_item.offset/Const.CHUNK_SIZE
                    if delta_item.delta_type == DeltaItem.DELTA_DISK or\
                            delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                        modified_disk_chunks.append(offset)
                    elif delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                        modified_memory_chunks.append(offset)
                    compressed_bytes = comp.compress(delta_bytes)
                    output_data += compressed_bytes
                    outdata_size_cur += len(compressed_bytes)
                    indata_size_cur += len(delta_bytes)
                    child_cur_block_count += 1

                compressed_bytes = comp.flush()
                output_data += compressed_bytes
                outdata_size_cur += len(compressed_bytes)
                time_process_end = time.clock()

                time_process_cur_time = (time_process_end - time_process_start)
                time_process_total_time += time_process_cur_time

                indata_size += indata_size_cur
                outdata_size += outdata_size_cur
                child_total_block += child_cur_block_count
                self.child_process_time_block.value = 1000.0*time_process_total_time/child_total_block
                self.child_ratio_block.value = outdata_size/float(indata_size)
                self.child_process_time_block_cur.value = 1000.0*time_process_cur_time/child_cur_block_count
                self.child_ratio_block_cur.value = outdata_size_cur/float(indata_size_cur)
                #print "%d in: %d, out: %d, %f %f" % (os.getpid(), indata_size_cur, outdata_size_cur, time_process_cur_time, self.child_process_time_block_cur.value)
                self.output_queue.put((comp_type_cur,
                                       output_data,
                                       modified_disk_chunks,
                                       modified_memory_chunks))

        sys.stdout.write("[Comp][Child] child finished. process %d jobs (%f)\n" % \
                         (loop_counter, time_process_total_time))
        self.command_queue.put((indata_size, outdata_size, child_total_block, time_process_total_time))
        while self.mode_queue.empty() == False:
            self.mode_queue.get_nowait()
            msg = "Empty new compression mode that does not refelected"
            sys.stdout.write(msg)


class DecompProc(multiprocessing.Process):
    def __init__(self, input_queue, output_queue, num_proc=4):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.num_proc = num_proc
        self.proc_list = list()
        multiprocessing.Process.__init__(self, target=self.decompress_blobs)

    def decompress_blobs(self):
        time_start = time.time()

        # launch child processes
        output_fd_list = list()
        output_fd_dict = dict()
        for i in range(self.num_proc):
            command_queue = multiprocessing.Queue()
            task_queue = multiprocessing.Queue(maxsize=1)
            comp_proc = DecompChildProc(command_queue, task_queue, self.output_queue)
            comp_proc.start()
            self.proc_list.append((comp_proc, task_queue, command_queue))
            output_fd_list.append(task_queue._writer.fileno())
            output_fd_dict[task_queue._writer.fileno()] = task_queue

        try:
            while True:
                recv_data = self.input_queue.get()
                if recv_data == Const.QUEUE_SUCCESS_MESSAGE:
                    break
                if recv_data == Const.QUEUE_FAILED_MESSAGE:
                    raise StreamSynthesisError("Failed to compress the blob")
                    break

                (comp_type, comp_data) = recv_data
                ([], output_ready, []) = select.select([], output_fd_list, [])
                task_queue = output_fd_dict[output_ready[0]]
                task_queue.put(recv_data)
        except Exception as e:
            sys.stdout.write("[decomp] Exception")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.output_queue.put(Const.QUEUE_FAILED_MESSAGE)

        # send end meesage to every process
        for (proc, t_queue, c_queue) in self.proc_list:
            t_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
            #sys.stdout.write("[Decomp] send end message to each child\n")

        # after this for loop, all processing finished, but child process still
        # alive until all data pass to the next step
        for (proc, t_queue, c_queue) in self.proc_list:
            c_queue.get()
        time_end = time.time()
        #sys.stdout.write("[Decomp] effetively finished\n")

        for (proc, t_queue, c_queue) in self.proc_list:
            #sys.stdout.write("[Comp] waiting to dump all data to the next stage\n")
            proc.join()
        # send end message after the next stage finishes processing
        self.output_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
        sys.stdout.write("[time] Decomp (%s~%s): %s s\n" % \
                (time_start, time_end, (time_end-time_start)))


class DecompChildProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, output_queue):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.output_queue = output_queue
        super(DecompChildProc, self).__init__(target=self._decomp)

    def _decomp(self):
        is_proc_running = True
        input_list = [self.task_queue._reader.fileno()]
        while is_proc_running:
            inready, outread, errready = select.select(input_list, [], [])
            if self.task_queue._reader.fileno() in inready:
                input_task = self.task_queue.get()
                if input_task == Const.QUEUE_SUCCESS_MESSAGE:
                    is_proc_running = False
                    break
                (comp_type, comp_data) = input_task

                if comp_type == Const.COMPRESSION_LZMA:
                    decompressor = lzma.LZMADecompressor()
                    decomp_data = decompressor.decompress(comp_data)
                    decomp_data += decompressor.flush()
                elif comp_type == Const.COMPRESSION_BZIP2:
                    decompressor = bz2.BZ2Decompressor()
                    decomp_data = decompressor.decompress(comp_data)
                elif comp_type == Const.COMPRESSION_GZIP:
                    decomp_data = zlib.decompress(comp_data, zlib.MAX_WBITS|16)
                else:
                    raise CompressionError("Not valid compression option")
                self.output_queue.put(decomp_data)
        #sys.stdout.write("[decomp][Child] child finished. send command queue msg\n")
        self.command_queue.put("Compressed processed everything")


def decomp_overlay(meta, output_path):
    meta_dict = msgpack.unpackb(open(meta, "r").read())
    decomp_start_time = time()
    comp_overlay_files = meta_dict[Const.META_OVERLAY_FILES]
    comp_overlay_files = [item[Const.META_OVERLAY_FILE_NAME] for item in comp_overlay_files]
    comp_overlay_files = [os.path.join(os.path.dirname(meta), item) for item in comp_overlay_files]
    overlay_file = open(output_path, "w+b")
    for comp_file in comp_overlay_files:
        decompressor = lzma.LZMADecompressor()
        comp_data = open(comp_file, "r").read()
        decomp_data = decompressor.decompress(comp_data)
        decomp_data += decompressor.flush()
        overlay_file.write(decomp_data)
    sys.stdout.write("Overlay decomp time for %d files: %f at %s\n" % \
            (len(comp_overlay_files), (time()-decomp_start_time), output_path))
    overlay_file.close()

    return meta_dict


def decomp_overlayzip(overlay_path, outfilename):
    overlay_package = VMOverlayPackage(overlay_path)
    meta_raw = overlay_package.read_meta()
    meta_info = msgpack.unpackb(meta_raw)
    comp_overlay_files = meta_info[Const.META_OVERLAY_FILES]
    #disk_chunks = list()
    #memory_chunks = list()


    out_fd = open(outfilename, "w+b")
    for blob_info in comp_overlay_files:
        comp_filename = blob_info[Const.META_OVERLAY_FILE_NAME]
        comp_type = blob_info.get(Const.META_OVERLAY_FILE_COMPRESSION, Const.COMPRESSION_LZMA)
        #disk_chunks += blob_info.get(Const.META_OVERLAY_FILE_DISK_CHUNKS)
        #memory_chunks += blob_info.get(Const.META_OVERLAY_FILE_MEMORY_CHUNKS)
        sys.stdout.write("Decompression type: %d\n" % comp_type)
        if comp_type == Const.COMPRESSION_LZMA:
            comp_data = overlay_package.read_blob(comp_filename)
            decompressor = lzma.LZMADecompressor()
            decomp_data = decompressor.decompress(comp_data)
            decomp_data += decompressor.flush()
            out_fd.write(decomp_data)
        elif comp_type == Const.COMPRESSION_BZIP2:
            comp_data = overlay_package.read_blob(comp_filename)
            decompressor = bz2.BZ2Decompressor()
            decomp_data = decompressor.decompress(comp_data)
            out_fd.write(decomp_data)
        elif comp_type == Const.COMPRESSION_GZIP:
            comp_data = overlay_package.read_blob(comp_filename)
            decomp_data = zlib.decompress(comp_data, zlib.MAX_WBITS|16)
            out_fd.write(decomp_data)
        else:
            raise CompressionError("Not valid compression option")
    #disk_index = [((item*4096)<<1) | (0x02 & 0x0F) for item in disk_chunks]
    #memory_index = [((item*4096)<<1) | (0x01 & 0x0F) for item in memory_chunks]
    #mem_set = set()
    #for index in memory_index:
    #    if index in mem_set:
    #        print 'dup'
    #    else:
    #        mem_set.add(index)

    out_fd.close()
    return meta_info

