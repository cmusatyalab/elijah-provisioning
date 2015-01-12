#!/usr/bin/env python

import os
import sys
import time

import lzma
import bz2
import zlib

def compress(algorithm, comp_level, data, blob_size):
    output_data = ''
    for index in range(0, len(data), blob_size):
        chunked_data = data[index:index+blob_size]
        comp = None
        if algorithm == "lzma":
            comp = lzma.LZMACompressor(options={'format':'xz', 'level':comp_level})
        elif algorithm == "bzip2":
            comp = bz2.BZ2Compressor(comp_level)
        elif algorithm == "gzip":
            comp = zlib.compressobj(comp_level, zlib.DEFLATED, zlib.MAX_WBITS | 16)
        else:
            raise Exception("Not supporting")
        output = comp.compress(chunked_data)
        output_data += output
        output_data += comp.flush()
    return output_data


if __name__ == "__main__":
    # set input workloads
    workloads = [
        ("moped", "../comp-blob-size/data/moped.uncomp"),
        ("speech", "../comp-blob-size/data/speech.uncomp"),
        ("fluid", "../comp-blob-size/data/fluid.uncomp"),
        ("face", "../comp-blob-size/data/face.uncomp"),
        ("mar", "../comp-blob-size/data/mar.uncomp"),
    ]
    smallest = 1*1024*1024 # 128 KB
    blob_size_list = [smallest << shift for shift in xrange(1)]
    for (name, filepath) in workloads:
        data = open(filepath, "rb").read()
        data_len = len(data)
        for algorithm in ["gzip", "bzip2", "lzma"]:
            for comp_level in xrange(5,6):
                for blob_size in blob_size_list:
                    if blob_size > data_len*2:
                        continue
                    time_start = time.time()
                    time_cpu_start = time.clock()
                    output = compress(algorithm, comp_level, data, blob_size)
                    time_end = time.time()
                    time_cpu_end = time.clock()
                    print "name:%s\talgorithm:%s\tlevel:%d\tblob_size:%d\tinput_size:%ld\toutput_size:%ld\tratio:%f\twall-time:%f\tcpu-time:%f" % \
                        (name, algorithm, comp_level, blob_size, data_len,
                         len(output), len(output)/float(data_len),
                         (time_end-time_start), (time_cpu_end-time_cpu_start))

        print ""

