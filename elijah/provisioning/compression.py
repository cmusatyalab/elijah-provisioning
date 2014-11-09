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

from lzma import LZMACompressor
from lzma import LZMADecompressor
from Configuration import Const
from package import VMOverlayPackage
import process_manager


BLOCK_SIZE = 1024*1024*1



class CompressionError(Exception):
    pass


def _bzip2_read_data(fd, result_queue, process_info_dict):
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
        fd.close()


def _bzip2_write_data(input_data_queue, fd, process_info_dict):
    try:
        while True:
            select.select([input_data_queue._reader.fileno()], [], [])
            delta_item = input_data_queue.get()
            if delta_item == Const.QUEUE_SUCCESS_MESSAGE:
                break
            data = delta_item.get_serialized()
            fd.write(data)
    except Exception, e:
        sys.stderr.write("%s\n" % str(e))
    finally:
        fd.close()


def _comp_lzma(delta_list_queue, comp_delta_queue, speed, num_cores):
    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
    comp = LZMACompressor(options={'format':'xz', 'level':1})
    original_length = 0
    comp_data_length = 0
    comp_delta_bytes = ''
    sys.stdout.write("[compression] start LZMA compression\n")

    count = 0
    while True:
        select.select([delta_list_queue._reader.fileno()], [], [])
        delta_item = delta_list_queue.get()
        if delta_item == Const.QUEUE_SUCCESS_MESSAGE:
            break
        delta_bytes = delta_item.get_serialized()
        original_length += len(delta_bytes)
        comp_delta_bytes = comp.compress(delta_bytes)
        comp_data_length += len(comp_delta_bytes)
        comp_delta_queue.put(comp_delta_bytes)
        count += 1

    comp_delta_bytes = comp.flush()
    if comp_delta_bytes is not None and len(comp_delta_bytes) > 0:
        comp_data_length += len(comp_delta_bytes)
        comp_delta_queue.put(comp_delta_bytes)
    comp_delta_queue.put(Const.QUEUE_SUCCESS_MESSAGE)


class CompressProc(process_manager.ProcWorker):
    def __init__(self, delta_list_queue, comp_delta_queue, comp_type, comp_option=dict()):
        """
        comparisons of compression algorithm
        http://pokecraft.first-world.info/wiki/Quick_Benchmark:_Gzip_vs_Bzip2_vs_LZMA_vs_XZ_vs_LZ4_vs_LZO
        """
        self.delta_list_queue = delta_list_queue
        self.comp_delta_queue = comp_delta_queue
        self.comp_type = comp_type
        self.comp_option = comp_option
        super(CompressProc, self).__init__(target=self.compress_stream)

    def compress_stream(self):
        time_start = time.time()

        if self.comp_type == Const.COMPRESSION_LZMA:
            _comp_lzma(self.delta_list_queue, self.comp_delta_queue, 1, 1)
        elif self.comp_type == Const.COMPRESSION_BZIP2:
            speed = 5
            num_cores = 4
            self._comp_bzip2(speed, num_cores)
        elif self.comp_type == Const.COMPRESSION_GZIP:
            raise CompressionError("Not implemented")
        else:
            raise CompressionError("Not valid compression option")
        time_end = time.time()

        sys.stdout.write("[time] Overlay compression time (%f ~ %f): %f\n" % (
            time_start, time_end, (time_end-time_start)))


    def _comp_bzip2(self, speed, num_cores):
        cmd = "pbzip2 -c -%d -p%d" % (speed, num_cores)
        proc = None
        try:
            sys.stdout.write("[compression] start bzip2 compression %d %d\n" % (speed, num_cores))
            _PIPE = subprocess.PIPE
            proc = subprocess.Popen(cmd.split(" "), stdin=_PIPE, stdout=_PIPE, close_fds=True)
            write_t = threading.Thread(target=_bzip2_write_data,
                                    args=(self.delta_list_queue,
                                          proc.stdin,
                                          self.process_info))
            read_t = threading.Thread(target=_bzip2_read_data,
                                    args=(proc.stdout,
                                          self.comp_delta_queue,
                                          self.process_info))
            write_t.start()
            read_t.start()
            sys.stdout.write("[compression] waiting to fininsh compression proc\n")

            write_t.join()
            read_t.join()
            ret_code = proc.wait()
        except Exception as e:
            sys.stdout.write("[compression] Exception1n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.comp_delta_queue.put(Const.QUEUE_FAILED_MESSAGE)
            if proc is not None:
                proc.terminate()
            sys.exit(1)


def decomp_overlay(meta, output_path):
    meta_dict = msgpack.unpackb(open(meta, "r").read())
    decomp_start_time = time()
    comp_overlay_files = meta_dict[Const.META_OVERLAY_FILES]
    comp_overlay_files = [item[Const.META_OVERLAY_FILE_NAME] for item in comp_overlay_files]
    comp_overlay_files = [os.path.join(os.path.dirname(meta), item) for item in comp_overlay_files]
    overlay_file = open(output_path, "w+b")
    for comp_file in comp_overlay_files:
        decompressor = LZMADecompressor()
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
            decompressor = LZMADecompressor()
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




