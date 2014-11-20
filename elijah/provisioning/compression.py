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

from delta import DeltaItem

import lzma
import bz2
from Configuration import Const
from package import VMOverlayPackage
import process_manager



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
        for (proc, t_queue, c_queue, m_queue) in self.proc_list:
            if proc.is_alive() == True:
                m_queue.put(new_mode)

    def _chunk_blob(self):
        modified_disk_chunks = list()
        modified_memory_chunks = list()
        input_size = 0
        is_last_blob = False
        input_data = ''
        input_list = [self.control_queue._reader.fileno(),
                        self.delta_list_queue._reader.fileno()]
        while input_size < self.block_size:
            (input_ready, [], []) = select.select(input_list, [], [])
            if self.control_queue._reader.fileno() in input_ready:
                control_msg = self.control_queue.get()
                ret = self._handle_control_msg(control_msg)
                if ret == False:
                    if control_msg == "change_mode":
                        new_mode = self.control_queue.get()
                        self.change_mode(new_mode)
            if self.delta_list_queue._reader.fileno() in input_ready:
                deltaitem_list = self.delta_list_queue.get()
                if deltaitem_list == Const.QUEUE_SUCCESS_MESSAGE:
                    is_last_blob = True
                    break
                for delta_item in deltaitem_list:
                    delta_bytes = delta_item.get_serialized()
                    offset = delta_item.offset/Const.CHUNK_SIZE
                    if delta_item.delta_type == DeltaItem.DELTA_DISK:
                        modified_disk_chunks.append(offset)
                    elif delta_item.delta_type == DeltaItem.DELTA_MEMORY:
                        modified_memory_chunks.append(offset)
                    input_data += delta_bytes
                    input_size += len(delta_bytes)
        return is_last_blob, input_data, input_size, modified_disk_chunks, modified_memory_chunks

    def compress_stream(self):
        time_start = time.time()

        # launch child processes
        output_fd_list = list()
        output_fd_dict = dict()
        for i in range(self.num_proc):
            command_queue = multiprocessing.Queue()
            mode_queue = multiprocessing.Queue()
            task_queue = multiprocessing.Queue(maxsize=1)
            comp_proc = CompChildProc(command_queue, task_queue, mode_queue,
                                      self.comp_delta_queue,
                                      self.comp_type,
                                      self.comp_level)
            comp_proc.start()
            self.proc_list.append((comp_proc, task_queue, command_queue, mode_queue))
            output_fd_list.append(task_queue._writer.fileno())
            output_fd_dict[task_queue._writer.fileno()] = task_queue

        is_last_blob = False
        total_read_size = 0
        while is_last_blob == False:
            # read data
            is_last_blob, input_data, input_size, modified_disk_chunks, modified_memory_chunks = self._chunk_blob()

            if input_size > 0:
                ([], output_ready, []) = select.select([], output_fd_list, [])
                task_queue = output_fd_dict[output_ready[0]]
                total_read_size += input_size
                task_queue.put((input_data, modified_disk_chunks, modified_memory_chunks))

        # send end meesage to every process
        for (proc, t_queue, c_queue, m_queue) in self.proc_list:
            t_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
            #sys.stdout.write("[Comp] send end message to each child\n")

        # after this for loop, all processing finished, but child process still
        # alive until all data pass to the next step
        finished_proc_dict = dict()
        input_list = [self.control_queue._reader.fileno()]
        for (proc, t_queue, c_queue, m_queue) in self.proc_list:
            fileno = c_queue._reader.fileno()
            input_list.append(fileno)
            finished_proc_dict[fileno] = (c_queue, t_queue)
        while len(finished_proc_dict.keys()) > 0:
            #print "left proc number: %s" % (finished_proc_dict.keys())
            #for ffno, (cq, tq) in finished_proc_dict.iteritems():
            #    print "task quesize at %d: %d" % (ffno, tq.qsize())

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
                    (cq, tq) = finished_proc_dict[in_queue]
                    cq.get()
                    del finished_proc_dict[in_queue]
        self.process_info['is_alive'] = False

        time_end = time.time()
        #sys.stdout.write("[Comp] effetively finished\n")

        for (proc, t_queue, c_queue, m_queue) in self.proc_list:
            #sys.stdout.write("[Comp] waiting to dump all data to the next stage\n")
            proc.join()
        # send end message after the next stage finishes processing
        self.comp_delta_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
        sys.stdout.write("[time][compression] thread(%d), block(%d), level(%d), compression time (%f ~ %f): %f MB, %f MBps, %f s\n" % (
            self.num_proc, self.block_size, self.comp_level, time_start, time_end, total_read_size/1024.0/1024, 
            total_read_size/(time_end-time_start)/1024.0/1024, (time_end-time_start)))



class CompChildProc(multiprocessing.Process):
    def __init__(self, command_queue, task_queue, mode_queue,
                 output_queue, comp_type, comp_level):
        self.command_queue = command_queue
        self.task_queue = task_queue
        self.mode_queue = mode_queue
        self.output_queue = output_queue
        self.comp_type = comp_type
        self.comp_level = comp_level
        super(CompChildProc, self).__init__(target=self._comp)

    def _comp(self):
        is_proc_running = True
        input_list = [self.task_queue._reader.fileno(),
                      self.mode_queue._reader.fileno()]
        while is_proc_running:
            inready, outread, errready = select.select(input_list, [], [])
            if self.mode_queue._reader.fileno() in inready:
                # change mode
                new_mode = self.mode_queue.get()
                new_comp_type = new_mode.get("comp_type", None)
                new_comp_level = new_mode.get("comp_level", None)
                sys.stdout.write("Change Compression mode: from (%d, %d) to (%d, %d)\n" %
                                 (self.comp_type, self.comp_level, new_comp_type,
                                  new_comp_level))
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
                (input_data, modified_disk_chunks, modified_memory_chunks) = input_task

                if self.comp_type == Const.COMPRESSION_LZMA:
                    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
                    comp = lzma.LZMACompressor(options={'format':'xz', 'level':self.comp_level})
                    output_data = comp.compress(input_data)
                    output_data += comp.flush()
                elif self.comp_type == Const.COMPRESSION_BZIP2:
                    comp = bz2.BZ2Compressor(self.comp_level)
                    output_data = comp.compress(input_data)
                    output_data += comp.flush()
                else:
                    raise CompressionError("Not supporting")

                self.output_queue.put((self.comp_type, output_data, modified_disk_chunks, modified_memory_chunks))
        #sys.stdout.write("[Comp][Child] child finished. send command queue msg\n")
        self.command_queue.put("Compressed processed everything")
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
                    decomp_data += decompressor.flush()
                elif comp_type == Const.COMPRESSION_GZIP:
                    raise CompressionError("Not implemented")
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
            _PIPE = subprocess.PIPE
            proc = subprocess.Popen("pbzip2 -d".split(" "), close_fds=True,
                                    stdin=_PIPE, stdout=_PIPE, stderr=_PIPE)
            decomp_data, err = proc.communicate(input=comp_data)
            if err:
                sys.stderr.write("Error in getting free memory : %s\n" % str(err))
            out_fd.write(decomp_data)
        elif comp_type == Const.COMPRESSION_GZIP:
            raise CompressionError("Not implemented")
        else:
            raise CompressionError("Not valid compression option")
    #disk_index = [((item*4096)<<1) | (0x02 & 0x0F) for item in disk_chunks]
    #memory_index = [((item*4096)<<1) | (0x01 & 0x0F) for item in memory_chunks]

    out_fd.close()
    return meta_info


def _decomp_bzip2(overlay_file):
    pass




