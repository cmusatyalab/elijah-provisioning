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


BLOCK_SIZE = 1024*1024*1



class CompressionError(Exception):
    pass


def _bzip2_read_data(fd, result_queue, control_queue, process_info_dict):
    is_first_recv = False
    time_first_recv = 0
    time_process_finish = 0
    time_process_start = 0
    time_prev_report = 0
    processed_datasize = 0
    processed_duration = float(0)
    UPDATE_PERIOD = process_info_dict['update_period']
    try:
        while True:
            time_process_start = time.time()
            rlist, wlist, xlist = select.select([fd], [fd], [fd])
            if fd in rlist:
                data = os.read(fd.fileno(), BLOCK_SIZE)
                if not data:
                    break
                if is_first_recv == False:
                    is_first_recv = True
                    time_first_recv = time.time()
                result_queue.put(data)

                # measurement
                time_process_finish = time.time()
                processed_datasize += len(data)
                processed_duration += (time_process_finish - time_process_start)
                #print "aa: %f %f" % (processed_datasize, processed_duration)
                if (time_process_finish - time_prev_report) > UPDATE_PERIOD:
                    time_prev_report = time_process_finish
                    #print "compression BW: %f MBps" % \
                    #    (processed_datasize/processed_duration/1024.0/1024)
                    process_info_dict['current_bw'] = \
                        processed_datasize/processed_duration/1024.0/1024
                    processed_datasize = 0
                    processed_duration = float(0)
        result_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
        result_queue.put(Const.QUEUE_FAILED_MESSAGE)
        sys.exit(1)
    finally:
        sys.stdout.write("[time] compression first input at : %f" % (time_first_recv))
        fd.close()


def _bzip2_write_data(input_data_queue, fd, control_queue, response_queue):
    try:
        while True:
            input_list = [control_queue._reader.fileno(), input_data_queue._reader.fileno()]
            (input_ready, [], []) = select.select(input_list, [], [])
            if control_queue._reader.fileno() in input_ready:
                control_msg = control_queue.get()
                response_queue.put(0)
            if input_data_queue._reader.fileno() in input_ready:
                delta_item = input_data_queue.get()
                if delta_item == Const.QUEUE_SUCCESS_MESSAGE:
                    break
                data = delta_item.get_serialized()
                fd.write(data)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
    finally:
        fd.close()



class CompressProc(process_manager.ProcWorker):
    def __init__(self, delta_list_queue, comp_delta_queue, comp_type,
                 num_threads=4, block_size=1024*1024*8, comp_level=4):
        """
        comparisons of compression algorithm
        http://pokecraft.first-world.info/wiki/Quick_Benchmark:_Gzip_vs_Bzip2_vs_LZMA_vs_XZ_vs_LZ4_vs_LZO
        """
        self.delta_list_queue = delta_list_queue
        self.comp_delta_queue = comp_delta_queue
        self.comp_type = comp_type
        self.num_threads = num_threads
        self.block_size = block_size
        self.comp_level = comp_level
        self.thread_list = list()
        super(CompressProc, self).__init__(target=self.compress_stream)

    def _chunk_blob(self, modified_memory_chunks, modified_disk_chunks):
        input_size = 0
        is_last_blob = False
        input_data = ''
        while input_size < self.block_size:
            input_list = [self.control_queue._reader.fileno(),
                            self.delta_list_queue._reader.fileno()]
            (input_ready, [], []) = select.select(input_list, [], [])
            if self.control_queue._reader.fileno() in input_ready:
                control_msg = control_queue.get()
                self._handle_control_msg(control_msg)
            if self.delta_list_queue._reader.fileno() in input_ready:
                delta_item = self.delta_list_queue.get()
                if delta_item == Const.QUEUE_SUCCESS_MESSAGE:
                    is_last_blob = True
                    break
                delta_bytes = delta_item.get_serialized()
                offset = delta_item.offset/Const.CHUNK_SIZE
                if delta_item.delta_type == DeltaItem.DELTA_DISK:
                    modified_disk_chunks.append(offset)
                elif delta_item.delta_type == DeltaItem.DELTA_MEMORY:
                    modified_memory_chunks.append(offset)
                input_data += delta_bytes
                input_size += len(delta_bytes)
        return is_last_blob, input_data, input_size


    def compress_stream(self):
        time_start = time.time()

        is_last_blob = False 
        total_read_size = 0
        while is_last_blob == False:
            # read data
            modified_memory_chunks = list()
            modified_disk_chunks = list()
            is_last_blob, input_data, input_size = self._chunk_blob(modified_memory_chunks, modified_disk_chunks)

            # new thread for data
            total_read_size += input_size
            if self.comp_type == Const.COMPRESSION_LZMA:
                new_thread = LZMAThread(input_data, self.comp_level)
            elif self.comp_type == Const.COMPRESSION_BZIP2:
                new_thread = BZIP2thread(input_data, self.comp_level)
            else:
                raise CompressionError("Not implemented")
            new_thread.start()
            self.thread_list.append((new_thread, modified_memory_chunks, modified_disk_chunks))
            #sys.stdout.write("[compression] new thread to compress %d data (total thread: %d)\n" % (input_size, len(self.thread_list)))
            if len(self.thread_list) == self.num_threads:
                (c_thread, m_memory_chunks, m_disk_chunks) = self.thread_list.pop(0)
                c_thread.join()
                data = c_thread.output_data
                self.comp_delta_queue.put(data)
                self.comp_delta_queue.put(m_memory_chunks)
                self.comp_delta_queue.put(m_disk_chunks)

        while len(self.thread_list) > 0:
            (c_thread, m_memory_chunks, m_disk_chunks) = self.thread_list.pop(0)
            c_thread.join()
            data = c_thread.output_data
            self.comp_delta_queue.put(data)
            self.comp_delta_queue.put(m_memory_chunks)
            self.comp_delta_queue.put(m_disk_chunks)
        self.comp_delta_queue.put(Const.QUEUE_SUCCESS_MESSAGE)
        time_end = time.time()

        sys.stdout.write("[time] thread(%d), block(%d), level(%d), compression time (%f ~ %f): %f s, %f MB, %f MBps\n" % (
            self.num_threads, self.block_size, self.comp_level, time_start, time_end, (time_end-time_start), total_read_size/1024.0/1024, 
            total_read_size/(time_end-time_start)/1024.0/1024))


class LZMAThread(threading.Thread):
    def __init__(self, input_data, comp_level):
        self.input_data = input_data
        self.output_data = None
        self.comp_level = comp_level
        threading.Thread.__init__(self, target=self._comp_lzma)

    def _comp_lzma(self):
        # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
        #print "start new thread to handle %d data" % len(input_data)
        comp = lzma.LZMACompressor(options={'format':'xz', 'level':self.comp_level})
        self.output_data = comp.compress(self.input_data)
        self.output_data += comp.flush()


class BZIP2thread(threading.Thread):
    def __init__(self, input_data, comp_level):
        self.input_data = input_data
        self.output_data = None
        self.comp_level = comp_level
        threading.Thread.__init__(self, target=self._comp_bzip2)

    def _comp_bzip2(self):
        comp = bz2.BZ2Compressor(self.comp_level)
        self.output_data = comp.compress(self.input_data)
        self.output_data += comp.flush()


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

    out_fd = open(outfilename, "w+b")
    for blob_info in comp_overlay_files:
        comp_filename = blob_info[Const.META_OVERLAY_FILE_NAME]
        comp_type = blob_info.get(Const.META_OVERLAY_FILE_COMPRESSION, Const.COMPRESSION_LZMA)
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
    out_fd.close()
    return meta_info


def _decomp_bzip2(overlay_file):
    pass




