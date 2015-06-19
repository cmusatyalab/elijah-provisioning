#!/usr/bin/env python
import os
import time
import sys
import select
import msgpack
import multiprocessing
import traceback
import ctypes

from .delta import DeltaItem

import lzma
import bz2
import zlib
from .configuration import Const
from .configuration import VMOverlayCreationMode
from .package import VMOverlayPackage
from . import process_manager
from . import log as logging

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
        self.num_proc = VMOverlayCreationMode.MAX_THREAD_NUM
        self.comp_type = overlay_mode.COMPRESSION_ALGORITHM_TYPE
        self.comp_level = overlay_mode.COMPRESSION_ALGORITHM_SPEED
        self.block_size = block_size
        self.proc_list = list()

        # monitor value specific to compression
        self.monitor_time_first_input_recved = multiprocessing.RawValue(
            ctypes.c_double, 0)

        super(CompressProc, self).__init__(target=self.compress_stream)

    def change_mode(self, new_mode):
        for (proc, c_queue, m_queue) in self.proc_list:
            if proc.is_alive():
                m_queue.put(("new_mode", new_mode))

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
                if not ret:
                    if control_msg == "change_mode":
                        new_mode = self.control_queue.get()
                        self.change_mode(new_mode)
                    elif control_msg == "change_cores":
                        num_cores = self.control_queue.get()
                        self.change_affinity(num_cores)
            if self.delta_list_queue._reader.fileno() in input_ready:
                deltaitem_list = self.delta_list_queue.get()
                if self.is_first_recv is False:
                    self.is_first_recv = True
                    self.time_first_recv = time.time()
                    self.monitor_time_first_input_recved.value = self.time_first_recv
                    LOG.debug(
                        "[time] Compression first input at : %f" %
                        (self.time_first_recv))
                if deltaitem_list == Const.QUEUE_SUCCESS_MESSAGE:
                    is_last_blob = True
                    break
                recved_size = 0
                for delta_item in deltaitem_list:
                    recved_size += (delta_item.data_len + 11 + 8)
                input_size += recved_size
                input_deltalist += deltaitem_list
        return is_last_blob, input_deltalist

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
        return avg_p/counter, avg_r/counter

    def compress_stream(self):
        self.total_block = 0
        self.total_time = 0
        self.is_first_recv = False
        self.time_first_recv = 0
        self.measure_history = list()
        self.measure_history_cur = list()
        try:
            time_start = time.time()

            # launch child processes
            self.task_queue = multiprocessing.Queue(
                maxsize=VMOverlayCreationMode.MAX_THREAD_NUM)
            for i in range(self.num_proc):
                command_queue = multiprocessing.Queue()
                mode_queue = multiprocessing.Queue()
                comp_proc = CompChildProc(
                    command_queue,
                    self.task_queue,
                    mode_queue,
                    self.comp_delta_queue,
                    self.comp_type,
                    self.comp_level)
                comp_proc.start()
                self.proc_list.append((comp_proc, command_queue, mode_queue))

            is_last_blob = False
            while is_last_blob is False:
                # read data
                is_last_blob, input_deltalist = self._chunk_blob()

                if len(input_deltalist) > 0:
                    self.task_queue.put(input_deltalist)
                    # measurement
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
                    [prev_m_time,
                     prev_process_time,
                     prev_block_count,
                     prev_insize,
                     prev_outsize] = prev_measure_values
                    if prev_process_time < total_process_time:
                        monitor_p = total_process_time/total_block_count
                        monitor_r = float(total_output_size)/total_input_size
                        monitor_insize = total_input_size
                        monitor_outsize = total_output_size
                        self.monitor_total_time_block.value = monitor_p
                        self.monitor_total_ratio_block.value = monitor_r
                        self.monitor_total_input_size.value = monitor_insize
                        self.monitor_total_output_size.value = monitor_outsize
                        cur_wall_time = time.time()
                        self.measure_history.append(
                            (cur_wall_time,
                             total_process_time,
                             total_block_count,
                             monitor_insize,
                             monitor_outsize))

                        # get cur value compared to prev value
                        cur_process_time = total_process_time - \
                            prev_process_time
                        cur_block_count = total_block_count - prev_block_count
                        cur_insize = monitor_insize - prev_insize
                        cur_outsize = monitor_outsize - prev_outsize
                        cur_p = cur_process_time/cur_block_count
                        cur_r = float(cur_outsize)/cur_insize
                        self.measure_history_cur.append(
                            (cur_wall_time, cur_p, cur_r))
                        avg_cur_p, avg_cur_r = self.averaged_value(
                            self.measure_history_cur, cur_wall_time)

                        self.monitor_total_time_block_cur.value = avg_cur_p
                        self.monitor_total_ratio_block_cur.value = avg_cur_r
                        self.monitor_total_input_size_cur.value = cur_insize
                        self.monitor_total_output_size_cur.value = cur_outsize
            self.finish_processing_input.value = True

            # send end meesage to every process
            for index in self.proc_list:
                self.task_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

            # after this for loop, all processing finished, but child process still
            # alive until all data pass to the next step
            finished_proc_dict = dict()
            input_list = [self.control_queue._reader.fileno()]
            for (proc, c_queue, m_queue) in self.proc_list:
                fileno = c_queue._reader.fileno()
                input_list.append(fileno)
                finished_proc_dict[fileno] = c_queue
            while len(finished_proc_dict.keys()) > 0:
                (input_ready, [], []) = select.select(input_list, [], [])
                for in_queue in input_ready:
                    if self.control_queue._reader.fileno() == in_queue:
                        control_msg = self.control_queue.get()
                        ret = self._handle_control_msg(control_msg)
                        if not ret:
                            if control_msg == "change_mode":
                                new_mode = self.control_queue.get()
                                self.change_mode(new_mode)
                    else:
                        cq = finished_proc_dict[in_queue]
                        (input_size,
                         output_size,
                         blocks,
                         processed_time) = cq.get()
                        self.in_size += input_size
                        self.out_size += output_size
                        self.total_block += blocks
                        self.total_time += processed_time
                        del finished_proc_dict[in_queue]
            self.is_processing_alive.value = False

            time_end = time.time()
            LOG.debug(
                "profiling\t%s\tsize\t%ld\t%ld\t%f" %
                (self.__class__.__name__,
                 self.in_size,
                 self.out_size,
                 (self.out_size /
                  float(
                      self.in_size))))
            LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f\t%f" %
                      (self.__class__.__name__, time_start, time_end,
                       (time_end-time_start), self.total_time))
            LOG.debug(
                "profiling\t%s\tblock-size\t%f\t%f\t%d" %
                (self.__class__.__name__,
                 float(self.in_size) / self.total_block,
                 float(self.out_size) / self.total_block,
                 self.total_block))
            LOG.debug(
                "profiling\t%s\tblock-time\t%f\t%f\t%f" %
                (self.__class__.__name__,
                 time_start, time_end,
                 self.total_time / self.total_block))

            for (proc, c_queue, m_queue) in self.proc_list:
                # waiting to dump all data to the next stage
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
        self.child_process_time_total = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.child_process_block_total = multiprocessing.RawValue(
            ctypes.c_double, 0)
        self.child_input_size_total = multiprocessing.RawValue(
            ctypes.c_ulong, 0)
        self.child_output_size_total = multiprocessing.RawValue(
            ctypes.c_ulong, 0)

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
                (command, value) = self.mode_queue.get()
                if command == "new_mode":
                    new_mode = value
                    new_comp_type = new_mode.get("comp_type", None)
                    new_comp_level = new_mode.get("comp_level", None)
                    LOG.debug(
                        "change-mode\t%fcomp\t(%s,%s) -> (%s,%s)" %
                        (time.time(), self.comp_type, self.comp_level,
                         new_comp_type, new_comp_level))
                    if new_comp_type is not None:
                        self.comp_type = new_comp_type
                    if new_comp_level is not None:
                        self.comp_level = new_comp_level
                elif command == "new_num_cores":
                    new_num_cores = value
                    if new_num_cores is not None:
                        VMOverlayCreationMode.set_num_cores(new_num_cores)
            if self.task_queue._reader.fileno() in inready:
                input_task = self.task_queue.get()
                if input_task == Const.QUEUE_SUCCESS_MESSAGE:
                    is_proc_running = False
                    break
                deltaitem_list = input_task
                comp_type_cur = self.comp_type
                loop_counter += 1

                # get compressor
                if comp_type_cur == Const.COMPRESSION_LZMA:
                    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output
                    # right after input
                    comp = lzma.LZMACompressor(
                        options={'format': 'xz',
                                 'level': self.comp_level})
                elif comp_type_cur == Const.COMPRESSION_BZIP2:
                    comp = bz2.BZ2Compressor(self.comp_level)
                elif comp_type_cur == Const.COMPRESSION_GZIP:
                    comp = zlib.compressobj(
                        self.comp_level, zlib.DEFLATED, zlib.MAX_WBITS | 16)
                else:
                    raise CompressionError("Not supporting")

                # compression for each block
                modified_memory_chunks = list()
                modified_disk_chunks = list()
                output_data = ''
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
                self.child_input_size_total.value = indata_size
                self.child_output_size_total.value = outdata_size
                self.child_process_time_total.value = 1000.0 * \
                    time_process_total_time
                self.child_process_block_total.value = child_total_block
                self.output_queue.put((comp_type_cur,
                                       output_data,
                                       modified_disk_chunks,
                                       modified_memory_chunks))

        # sys.stdout.write("[Comp][Child] child finished. process %d jobs (%f)\n" % \
        #                 (loop_counter, time_process_total_time))
        self.command_queue.put(
            (indata_size,
             outdata_size,
             child_total_block,
             time_process_total_time))
        while self.mode_queue.empty() is False:
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
            comp_proc = DecompChildProc(
                command_queue,
                task_queue,
                self.output_queue)
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
                    raise CompressionError("Failed to compress the blob")
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
            # send end message to each child
            t_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        # after this for loop, all processing finished, but child process still
        # alive until all data pass to the next step
        for (proc, t_queue, c_queue) in self.proc_list:
            c_queue.get()
        time_end = time.time()

        for (proc, t_queue, c_queue) in self.proc_list:
            proc.join()
        # send end message after the next stage finishes processing
        self.output_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
        sys.stdout.write(
            "[time] Decomp using %d proc (%s~%s): %s s\n" %
            (self.num_proc, time_start, time_end, (time_end-time_start)))


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
                    decomp_data = zlib.decompress(comp_data, zlib.MAX_WBITS | 16)
                else:
                    raise CompressionError("Not valid compression option")
                LOG.debug("%f\tdecompress one blob" % (time.time()))
                self.output_queue.put(decomp_data)
        self.command_queue.put("Compressed processed everything")


def decomp_overlay(meta, output_path):
    meta_dict = msgpack.unpackb(open(meta, "r").read())
    decomp_start_time = time()
    comp_overlay_files = meta_dict[Const.META_OVERLAY_FILES]
    comp_overlay_files = [item
                          [Const.META_OVERLAY_FILE_NAME]
                          for item in comp_overlay_files]
    comp_overlay_files = [os.path.join
                          (os.path.dirname(meta), item)
                          for item in comp_overlay_files]
    overlay_file = open(output_path, "w+b")
    for comp_file in comp_overlay_files:
        decompressor = lzma.LZMADecompressor()
        comp_data = open(comp_file, "r").read()
        decomp_data = decompressor.decompress(comp_data)
        decomp_data += decompressor.flush()
        overlay_file.write(decomp_data)
    sys.stdout.write(
        "Overlay decomp time for %d files: %f at %s\n" %
        (len(comp_overlay_files), (time()-decomp_start_time), output_path))
    overlay_file.close()

    return meta_dict


def decomp_overlayzip(overlay_path, outfilename):
    overlay_package = VMOverlayPackage(overlay_path)
    meta_raw = overlay_package.read_meta()
    meta_info = msgpack.unpackb(meta_raw)
    comp_overlay_files = meta_info[Const.META_OVERLAY_FILES]

    out_fd = open(outfilename, "w+b")
    for blob_info in comp_overlay_files:
        comp_filename = blob_info[Const.META_OVERLAY_FILE_NAME]
        comp_type = blob_info.get(
            Const.META_OVERLAY_FILE_COMPRESSION,
            Const.COMPRESSION_LZMA)
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
            decomp_data = zlib.decompress(comp_data, zlib.MAX_WBITS | 16)
            out_fd.write(decomp_data)
        else:
            raise CompressionError("Not valid compression option")

    out_fd.close()
    return meta_info
