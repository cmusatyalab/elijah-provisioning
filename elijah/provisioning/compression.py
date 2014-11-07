#!/usr/bin/env python
import os
import time
import select
import subprocess
import threading

from lzma import LZMACompressor
from Configuration import Const
import log as logging


LOG = logging.getLogger(__name__)
BLOCK_SIZE = 1024*1024*10



class CompressionError(Exception):
    pass


def _bzip2_read_data(fd, result_queue):
    while True:
        rlist, wlist, xlist = select.select([fd], [fd], [fd])
        if fd in rlist:
            data = os.read(fd.fileno(), BLOCK_SIZE)
            if not data:
                break
            result_queue.put(data)
    fd.close()
    result_queue.put(Const.QUEUE_SUCCESS_MESSAGE)


def _bzip2_write_data(input_data_queue, fd):
    while True:
        delta_item = input_data_queue.get()
        if delta_item == Const.QUEUE_SUCCESS_MESSAGE:
            break
        data = delta_item.get_serialized()
        fd.write(data)
    fd.close()


def _comp_bzip2(delta_list_queue, comp_delta_queue, speed, num_cores):
    LOG.debug("[compression] start bzip2 compression")
    cmd = "pbzip2 -c -%d -p%d" % (speed, num_cores)
    _PIPE = subprocess.PIPE
    proc = subprocess.Popen(cmd.split(" "), stdin=_PIPE, stdout=_PIPE, close_fds=True)

    write_t = threading.Thread(target=_bzip2_write_data,
                               args=(delta_list_queue, proc.stdin))
    write_t.start()
    read_t = threading.Thread(target=_bzip2_read_data,
                              args=(proc.stdout, comp_delta_queue))
    read_t.start()

    #LOG.debug("[compression] waiting to fininsh writing data")
    write_t.join()
    #LOG.debug("[compression] waiting to fininsh reading data")
    read_t.join()
    LOG.debug("[compression] waiting to fininsh compression proc")
    ret_code = proc.wait()


def _comp_lzma(delta_list_queue, comp_delta_queue, speed, num_cores):
    # mode = 2 indicates LZMA_SYNC_FLUSH, which show all output right after input
    comp = LZMACompressor(options={'format':'xz', 'level':1})
    original_length = 0
    comp_data_length = 0
    comp_delta_bytes = ''
    LOG.debug("[compression] start LZMA compression")

    count = 0
    while True:
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


def compress_stream(delta_list_queue, comp_delta_queue, comp_type, comp_option=dict()):
    """
    comparisons of compression algorithm
    http://pokecraft.first-world.info/wiki/Quick_Benchmark:_Gzip_vs_Bzip2_vs_LZMA_vs_XZ_vs_LZ4_vs_LZO
    """
    start_time = time.time()
    if comp_type == Const.COMPRESSION_LZMA:
        _comp_lzma(delta_list_queue, comp_delta_queue, 1, 1)
    elif comp_type == Const.COMPRESSION_BZIP2:
        _comp_bzip2(delta_list_queue, comp_delta_queue, 7, 2)
    elif comp_type == Const.COMPRESSION_GZIP:
        raise CompressionError("Not implemented")
    else:
        raise CompressionError("Not valid compression option")
    end_time = time.time()

    LOG.debug("[time] Overlay compression time (%f ~ %f): %f" % (start_time, end_time, (end_time-start_time)))

