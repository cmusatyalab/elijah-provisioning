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

import xdelta3
import bsdiff4
import os
import filecmp
import sys
import subprocess
from time import time
from hashlib import sha1
from hashlib import sha256
import mmap
import struct
from lzma import LZMACompressor
from lzma import LZMADecompressor

import pyximport
pyximport.install()
from cython_xor import cython_xor

import msgpack
from .configuration import Const
from . import log as logging

LOG = logging.getLogger(__name__)


# global
_HASHFILE_MAGIC = 0x1145511a
_HASHFILE_VERSION = 0x00000001
_HASH_CHUNKING_SIZE = 4012
_LZMA_OPTION = {'format': 'xz', 'level': 9}


def diff_data(source_data, modi_data, buf_len):
    if len(source_data) == 0 or len(modi_data) == 0:
        raise IOError(
            "[Error] Not valid data length: %d, %d" %
            (len(source_data), len(modi_data)))

    result, patch = xdelta3.xd3_encode_memory(
        modi_data, source_data, buf_len, xdelta3.XD3_COMPLEVEL_9)
    if result != 0:
        msg = "Error while xdelta3: %d" % result
        raise IOError(msg)
    return patch


def diff_data_bsdiff(source_data, modi_data):
    if len(source_data) == 0 or len(modi_data) == 0:
        raise IOError(
            "[Error] Not valid data length: %d, %d" %
            (len(source_data), len(modi_data)))

    patch = bsdiff4.diff(source_data, modi_data)
    return patch


def merge_data(source_data, overlay_data, buf_len):
    if len(source_data) == 0 or len(overlay_data) == 0:
        raise IOError(
            "[Error] Not valid data length: %d, %d" %
            (len(source_data), len(overlay_data)))
    result, recover = xdelta3.xd3_decode_memory(
        overlay_data, source_data, buf_len)
    if result != 0:
        raise IOError("Error while xdelta3 : %d" % result)
    return recover


def merge_data_bsdiff(source_data, overlay_data):
    if len(source_data) == 0 or len(overlay_data) == 0:
        raise IOError(
            "[Error] Not valid data length: %d, %d" %
            (len(source_data), len(overlay_data)))
    recover = bsdiff4.patch(source_data, overlay_data)
    return recover


def comp_lzma(inputname, outputname, **kwargs):
    log = kwargs.get("log", None)
    nova_util = kwargs.get('nova_util', None)

    prev_time = time()
    fin = open(inputname, 'rb')
    fout = open(outputname, 'wb')
    if nova_util:
        (stdout, stderr) = nova_util.execute(
            'xz', '-9cv', process_input=fin.read())
        fout.write(stdout)
    else:
        ret = subprocess.call(['xz', '-9cv'], stdin=fin, stdout=fout)
        if ret:
            raise IOError('XZ compressor failed')

    fin.close()
    fout.close()
    time_diff = str(time()-prev_time)
    return outputname, str(time_diff)


def decomp_lzma(inputname, outputname, **kwargs):
    log = kwargs.get("log", None)
    nova_util = kwargs.get('nova_util', None)

    prev_time = time()
    fin = open(inputname, 'rb')
    fout = open(outputname, 'wb')
    if nova_util:
        (stdout, stderr) = nova_util.execute(
            'xz', '-d', process_input=fin.read())
        fout.write(stdout)
    else:
        ret = subprocess.call(['xz', '-d'], stdin=fin, stdout=fout)
        if ret:
            raise IOError('XZ decompressor failed')
    fin.close()
    fout.close()

    time_diff = str(time()-prev_time)
    return outputname, str(time_diff)

    return meta_dict


if __name__ == "__main__":
    import random
    import string

    if sys.argv[1] == "comp":
        base = ''.join(random.choice
                       (string.ascii_uppercase + string.digits)
                       for x in range(2096))
        compressor = LZMACompressor(_LZMA_OPTION)
        comp = compressor.compress(base)
        comp += compressor.flush()

        decompressor = LZMADecompressor()
        decomp = decompressor.decompress(comp)
        decomp += decompressor.flush()

        if base != decomp:
            print "result is wrong"
            print "%d == %d" % (len(base), len(decomp))
            sys.exit(1)
        print "success"

    elif sys.argv[1] == "xdelta":
        base = ''.join(random.choice
                       (string.ascii_uppercase + string.digits)
                       for x in range(4096))
        modi = "~"*4096
        patch = diff_data(base, modi, len(base))
        recover = merge_data(base, patch, len(base))

        if sha256(modi).digest() == sha256(recover).digest():
            print "SUCCESS"
            print len(patch)
        else:
            print "Failed %d == %d" % (len(modi), len(recover))
