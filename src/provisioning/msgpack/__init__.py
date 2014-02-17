# coding: utf-8
from _version import version
from exceptions import *

import os
if os.environ.get('MSGPACK_PUREPYTHON'):
    from fallback import pack, packb, Packer, unpack, unpackb, Unpacker
else:
    try:
        from _packer import pack, packb, Packer
        from _unpacker import unpack, unpackb, Unpacker
    except ImportError:
        from fallback import pack, packb, Packer, unpack, unpackb, Unpacker

# alias for compatibility to simplejson/marshal/pickle.
load = unpack
loads = unpackb

dump = pack
dumps = packb

