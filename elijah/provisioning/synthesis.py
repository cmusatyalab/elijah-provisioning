#!/usr/bin/env python
#
# cloudlet infrastructure for mobile computing
#
#   author: kiryong ha <krha@cmu.edu>
#
#   copyright (c) 2011-2013 carnegie mellon university
#   licensed under the apache license, version 2.0 (the "license");
#   you may not use this file except in compliance with the license.
#   you may obtain a copy of the license at
#
#       http://www.apache.org/licenses/license-2.0
#
#   unless required by applicable law or agreed to in writing, software
#   distributed under the license is distributed on an "as is" basis,
#   without warranties or conditions of any kind, either express or implied.
#   see the license for the specific language governing permissions and
#   limitations under the license.
#

import sys
import os
import select
import functools
import subprocess
import hashlib
import libvirt
import shutil
import struct
import json
import msgpack
import signal
import threading
import traceback
from operator import itemgetter
from urlparse import urlsplit
from distutils.version import LooseVersion
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from uuid import uuid4
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from time import time
from time import sleep
from optparse import OptionParser
import datetime

from . import memory
from . import disk
from . import cloudletfs
from . import memory_util
from . import delta
from .db import api as db_api
from .db import table_def as db_table
from .db.api import update_op, log_op
from .configuration import Const
from .configuration import Options
from .delta import DeltaList
from .delta import DeltaItem
from .progressbar import AnimatedProgressBar
from .package import VMOverlayPackage
from . import handoff
from . import qmp_af_unix
from .tool import comp_lzma
from . import compression
import logging
import elijah.provisioning.db.table_def as table_def
from elijah.provisioning.db.api import DBConnector
from elijah.provisioning.package import PackagingUtil


# to work with OpenStack's eventlet
try:
    from eventlet import patcher
    if patcher.is_monkey_patched("thread"):
        native_threading = patcher.original("threading")
    else:
        raise ImportError("threading is not monkey-patched")
except ImportError as e:
    import threading
    native_threading = threading


LOG = logging.getLogger(__name__)
HANDOFF_SIGNAL_RECEIVED = False

class CloudletGenerationError(Exception):
    pass


def wrap_vm_fault(function):
    """Wraps a method to catch exceptions related to instances.
    This decorator wraps a method to catch any exceptions and
    terminate the request gracefully.
    """
    @functools.wraps(function)
    def decorated_function(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)
        except Exception as e:
            kwargs.update(dict(zip(function.func_code.co_varnames[2:], args)))
            LOG.error(
                "failed : reasons - %s, args - %s" %
                (str(e), str(kwargs)))
            LOG.error("failed at %s" % str(traceback.format_exc()))
            if hasattr(self, 'exception_handler'):
                self.exception_handler()
            raise e

    return decorated_function


def libvirt_err_callback(ctxt, err):
    # we intentionally ignore seek error from libvirt since we have cause
    # that by using named pipe
    if err[3] == libvirt.VIR_ERR_ERROR and \
            err[0] == libvirt.VIR_FROM_STREAMS and \
            err[1] == libvirt.VIR_FROM_QEMU:
        pass
    elif err[3] == libvirt.VIR_ERR_WARNING:
        LOG.warning(err[2])
    elif err[3] == libvirt.VIR_ERR_ERROR:
        LOG.error(err[2])

libvirt.registerErrorHandler(f=libvirt_err_callback, ctx=None)


class VM_Overlay(native_threading.Thread):

    def __init__(self, base_disk, options, qemu_args=None,
                 base_mem=None, base_diskmeta=None,
                 base_memmeta=None, base_hashvalue=None,
                 nova_xml=None, nova_util=None, nova_conn=None):
        """create VM overlay from a user-customized VM.
        """
        self.base_disk = os.path.abspath(base_disk)
        self.options = options
        self.nova_xml = nova_xml or None
        self.qemu_args = qemu_args or None
        (self.base_diskmeta, self.base_mem, self.base_memmeta) = \
            Const.get_basepath(self.base_disk, check_exist=False)
        self.base_mem = base_mem or self.base_mem
        self.base_diskmeta = base_diskmeta or self.base_diskmeta
        self.base_memmeta = base_memmeta or self.base_memmeta
        # use abs path
        self.base_mem = os.path.abspath(self.base_mem)
        self.base_diskmeta = os.path.abspath(self.base_diskmeta)
        self.base_memmeta = os.path.abspath(self.base_memmeta)
        self.nova_util = nova_util
        self.conn = nova_conn or get_libvirt_connection()

        # find base vm's hashvalue from DB
        self.base_hashvalue = base_hashvalue or None
        if self.base_hashvalue is None:
            dbconn = db_api.DBConnector()
            basevm_list = dbconn.list_item(db_table.BaseVM)
            for basevm_row in basevm_list:
                basevm_row_disk = os.path.abspath(basevm_row.disk_path)
                if basevm_row_disk == self.base_disk:
                    self.base_hashvalue = basevm_row.hash_value
            if self.base_hashvalue is None:
                msg = "Cannot find hashvalue for %s" % self.base_disk
                raise CloudletGenerationError(msg)

        self.log_path = os.path.join(
            os.path.dirname(self.base_disk),
            os.path.basename(self.base_disk) + Const.OVERLAY_LOG
        )
        native_threading.Thread.__init__(self, target=self.create_overlay)

    @wrap_vm_fault
    def resume_basevm(self, title=None):
        if (self.options is None) or (isinstance(self.options, Options) == False):
            msg = "Given option class is invalid: %s" % str(self.options)
            raise CloudletGenerationError(msg)

        # filename for overlay VM
        temp_qemu_dir = mkdtemp(prefix="cloudlet-qemu-")
        self.qemu_logfile = os.path.join(temp_qemu_dir, "qemu-trim-log")
        # change permission of the file
        for qemu_file in [self.qemu_logfile]:
            open(qemu_file, "w+").close()
            os.chmod(os.path.dirname(qemu_file), 0o777)
            os.chmod(qemu_file, 0o666)
            LOG.info("QEMU access file : %s" % qemu_file)

        # make FUSE disk & memory
        self.fuse = run_fuse(Const.CLOUDLETFS_PATH, Const.CHUNK_SIZE,
                             self.base_disk, os.path.getsize(self.base_disk),
                             self.base_mem, os.path.getsize(self.base_mem))
        self.modified_disk = os.path.join(
            self.fuse.mountpoint,
            'disk',
            'image')
        self.base_mem_fuse = os.path.join(
            self.fuse.mountpoint,
            'memory',
            'image')
        self.modified_mem = NamedTemporaryFile(
            prefix="cloudlet-mem-",
            delete=False)
        # monitor modified chunks
        stream_modified = os.path.join(self.fuse.mountpoint,
                                       'disk',
                                       'streams',
                                       'chunks_modified')
        stream_access = os.path.join(self.fuse.mountpoint,
                                     'disk',
                                     'streams',
                                     'chunks_accessed')
        memory_access = os.path.join(self.fuse.mountpoint,
                                     'memory',
                                     'streams',
                                     'chunks_accessed')
        self.fuse_stream_monitor = cloudletfs.StreamMonitor()
        self.fuse_stream_monitor.add_path(
            stream_modified, cloudletfs.StreamMonitor.DISK_MODIFY)
        self.fuse_stream_monitor.add_path(
            stream_access, cloudletfs.StreamMonitor.DISK_ACCESS)
        self.fuse_stream_monitor.add_path(
            memory_access, cloudletfs.StreamMonitor.MEMORY_ACCESS)
        self.fuse_stream_monitor.start()
        self.qemu_monitor = cloudletfs.FileMonitor(
            self.qemu_logfile, cloudletfs.FileMonitor.QEMU_LOG)
        self.qemu_monitor.start()

        # resume & get modified disk
        LOG.info("* Overlay creation configuration")
        LOG.info("  - %s" % str(self.options))
        self.old_xml_str, self.new_xml_str = _convert_xml(
            self.modified_disk, mem_snapshot=self.base_mem_fuse,
            qemu_logfile=self.qemu_logfile, qemu_args=self.qemu_args,
            nova_xml=self.nova_xml, title=title, operation='Generate customized snapshot')
        self.machine = run_snapshot(self.conn, self.modified_disk,
                                    self.base_mem_fuse, self.new_xml_str)
        return self.machine

    @wrap_vm_fault
    def create_overlay(self):
        # get montoring info
        monitoring_info = _get_overlay_monitoring_info(
            self.conn,
            self.machine,
            self.options,
            self.base_memmeta,
            self.base_diskmeta,
            self.fuse_stream_monitor,
            self.base_disk,
            self.base_mem,
            self.modified_disk,
            self.modified_mem.name,
            self.qemu_logfile,
            nova_util=self.nova_util)

        # get overlay VM
        overlay_deltalist = get_overlay_deltalist(
            monitoring_info,
            self.options,
            self.base_disk,
            self.base_mem,
            self.base_memmeta,
            self.modified_disk,
            self.modified_mem.name)

        # create_overlayfile
        temp_dir = mkdtemp(prefix="cloudlet-overlay-")
        overlay_prefix = os.path.join(temp_dir, Const.OVERLAY_FILE_PREFIX)
        overlay_metapath = os.path.join(temp_dir, Const.OVERLAY_META)

        self.overlay_metafile, self.overlay_files = generate_overlayfile(
            overlay_deltalist,
            self.options,
            self.base_hashvalue,
            os.path.getsize(self.modified_disk),
            os.path.getsize(self.modified_mem.name),
            overlay_metapath,
            overlay_prefix)
        # packaging VM overlay into a single zip file
        if self.options.ZIP_CONTAINER:
            self.overlay_zipfile = os.path.join(temp_dir, Const.OVERLAY_ZIP)
            VMOverlayPackage.create(
                self.overlay_zipfile,
                self.overlay_metafile,
                self.overlay_files)
            # delete tmp overlay files
            if os.path.exists(self.overlay_metafile):
                os.remove(self.overlay_metafile)
            for overlay_file in self.overlay_files:
                if os.path.exists(overlay_file):
                    os.remove(overlay_file)

        # terminate
        self.terminate()

    def terminate(self):
        if hasattr(self, 'fuse_stream_monitor') and\
                (self.fuse_stream_monitor is not None):
            self.fuse_stream_monitor.terminate()
            self.fuse_stream_monitor.join()
        if hasattr(self, 'fuse') and self.fuse is not None:
            self.fuse.terminate()
            self.fuse.join()
        if hasattr(self, 'qemu_monitor') and self.qemu_monitor is not None:
            self.qemu_monitor.terminate()
            self.qemu_monitor.join()
        if os.path.exists(self.modified_mem.name):
            os.unlink(self.modified_mem.name)

        # delete cloudlet-qemu-log directory
        if os.path.exists(os.path.dirname(self.qemu_logfile)):
            shutil.rmtree(os.path.dirname(self.qemu_logfile))
        if hasattr(self, 'machine'):
            _terminate_vm(self.conn, self.machine)
            self.machine = None

    def exception_handler(self):
        # make sure to destory the VM
        self.terminate()
        if hasattr(self, 'machine'):
            _terminate_vm(self.conn, self.machine)
            self.machine = None


class OverlayMonitoringInfo(object):
    BASEDISK_HASHLIST = "basedisk_hashlist"
    BASEMEM_HASHLIST = "basemem_hashlist"
    DISK_MODIFIED_BLOCKS = "disk_modified_block"  # from fuse monitoring
    DISK_USED_BLOCKS = "disk_used_block"  # from xray support
    DISK_FREE_BLOCKS = "disk_free_block"
    MEMORY_FREE_BLOCKS = "memory_free_block"

    def __init__(self, properties):
        for k, v in properties.iteritems():
            setattr(self, k, v)

    def __str__(self):
        ret = ""
        for k, v in self.__dict__.iteritems():
            ret += "%s\t:\t%s\n" % (str(k), str(v))
        return ret

    def __getitem__(self, item):
        return self.__dict__[item]


class SynthesizedVM(native_threading.Thread):

    def __init__(self, launch_disk, launch_mem, fuse,
                 disk_only=False, qemu_args=None, fwd_ports=None, **kwargs):
        # kwargs
        self.xml = kwargs.get("xml", None)
        self.title = kwargs.get("title", None)
        self.nova_util = kwargs.get("nova_util", None)
        self.conn = kwargs.get("nova_conn", None) or get_libvirt_connection()
        self.LOG = kwargs.get("log", None)
        if self.LOG is None:
            self.LOG = open("/dev/null", "w+b")

        # monitor modified chunks
        self.machine = None
        self.disk_only = disk_only
        self.qemu_args = qemu_args
        if fwd_ports is not None:
            self.fwd_ports = fwd_ports.split(',')
        else:
            self.fwd_ports = None
        self.fuse = fuse
        self.launch_disk = launch_disk
        self.launch_mem = launch_mem

        temp_qemu_dir = mkdtemp(prefix="cloudlet-overlay-")
        self.qemu_logfile = os.path.join(temp_qemu_dir, "qemu-trim-log")
        self.qmp_channel = os.path.abspath(
            os.path.join(temp_qemu_dir,"qmp-channel"))
        # change permission of the file
        for qemu_file in [self.qemu_logfile, self.qmp_channel]:
            open(qemu_file, "w+").close()
            os.chmod(os.path.dirname(qemu_file), 0o777)
            os.chmod(qemu_file, 0o666)
            LOG.info("QEMU access file : %s" % qemu_file)
        if os.path.exists(self.qmp_channel):
            os.remove(self.qmp_channel)
        LOG.info("Launch disk: %s" % os.path.abspath(launch_disk))
        LOG.info("Launch memory: %s" % os.path.abspath(launch_mem))
        LOG.info("QMP channel: %s" % self.qmp_channel)

        self.resumed_disk = os.path.join(fuse.mountpoint, 'disk', 'image')
        self.resumed_mem = os.path.join(fuse.mountpoint, 'memory', 'image')
        self.stream_modified = os.path.join(
            fuse.mountpoint, 'disk', 'streams', 'chunks_modified')
        self.stream_disk_access = os.path.join(
            fuse.mountpoint, 'disk', 'streams', 'chunks_accessed')
        self.stream_memory_access = os.path.join(
            fuse.mountpoint, 'memory', 'streams', 'chunks_accessed')
        self.monitor = cloudletfs.StreamMonitor()
        self.monitor.add_path(
            self.stream_modified, cloudletfs.StreamMonitor.DISK_MODIFY)
        self.monitor.add_path(
            self.stream_disk_access, cloudletfs.StreamMonitor.DISK_ACCESS)
        self.monitor.add_path(
            self.stream_memory_access, cloudletfs.StreamMonitor.MEMORY_ACCESS)
        self.monitor.start()
        self.qemu_monitor = cloudletfs.FileMonitor(
            self.qemu_logfile, cloudletfs.FileMonitor.QEMU_LOG)
        self.qemu_monitor.start()

        native_threading.Thread.__init__(self, target=self.resume)

    def _generate_xml(self):
        # convert xml
        if self.disk_only:
            self.old_xml_str, self.new_xml_str = _convert_xml(
                self.resumed_disk,
                qemu_logfile=self.qemu_logfile,
                qemu_args=self.qemu_args,
            )
        else:
            self.old_xml_str, self.new_xml_str = _convert_xml(
                self.resumed_disk,
                xml=ElementTree.fromstring(self.xml) if self.xml is not None else None,
                mem_snapshot=self.resumed_mem,
                qemu_logfile=self.qemu_logfile,
                qmp_channel=self.qmp_channel,
                qemu_args=self.qemu_args,
                memory_snapshot_mode="live",
                title=self.title,
                operation='Instantiated memory/disk snapshot',
                fwd_ports=self.fwd_ports
            )

    def resume(self):
        # resume VM
        self.resume_time = {'time': -100}
        self._generate_xml()
        try:
            if self.disk_only:
                # edit default XML to have new disk path
                self.machine = run_vm(self.conn, self.new_xml_str,
                                      vnc_disable=True)
            else:
                self.machine = run_snapshot(self.conn, self.resumed_disk,
                                            self.resumed_mem, self.new_xml_str,
                                            resume_time=self.resume_time)
        except Exception as e:
            sys.stdout.write(str(e)+"\n")

        if self.nova_util:
            self.nova_util.execute(
                'chmod', 775, self.qmp_channel, run_as_root=True)
        return self.machine

    def terminate(self):
        try:
            if hasattr(self, 'machine') and self.machine is not None:
                _terminate_vm(self.conn, self.machine)
                self.machine = None
        except libvirt.libvirtError as e:
            pass

        # terminate
        if self.monitor is not None:
            self.monitor.terminate()
            self.monitor.join()
        self.fuse.terminate()
        self.qemu_monitor.terminate()
        self.qemu_monitor.join()

        # delete all temporary file
        if os.path.exists(self.launch_disk):
            os.unlink(self.launch_disk)
        if os.path.exists(self.launch_mem):
            os.unlink(self.launch_mem)
        if os.path.exists(os.path.dirname(self.qemu_logfile)):
            shutil.rmtree(os.path.dirname(self.qemu_logfile))


def _terminate_vm(conn, machine):
    machine_id = machine.ID()
    try:
        for each_id in conn.listDomainsID():
            if each_id == machine_id:
                each_machine = conn.lookupByID(machine_id)
                vm_state, reason = each_machine.state(0)
                if vm_state != libvirt.VIR_DOMAIN_SHUTOFF:
                    each_machine.destroy()
    except libvirt.libvirtError as e:
        pass


def _update_overlay_meta(original_meta, new_path, blob_info=None):
    fout = open(new_path, "r+b")

    if blob_info:
        original_meta[Const.META_OVERLAY_FILES] = blob_info
    serialized = msgpack.packb(original_meta)
    fout.write(serialized)
    fout.close()


def _test_dma_accuracy(dma_dict, disk_deltalist, mem_deltalist):
    dma_start_time = time()
    dma_read_counter = 0
    dma_write_counter = 0
    dma_read_overlay_dedup = 0
    dma_write_overlay_dedup = 0
    dma_read_base_dedup = 0
    dma_write_base_dedup = 0
    disk_delta_dict = dict(
        [(delta.offset / Const.CHUNK_SIZE, delta) for delta in disk_deltalist])
    mem_delta_dict = dict(
        [(delta.offset/Const.CHUNK_SIZE, delta) for delta in mem_deltalist])
    for dma_disk_chunk in dma_dict.keys():
        item = dma_dict.get(dma_disk_chunk)
        is_dma_read = item['read']
        dma_mem_chunk = item['mem_chunk']
        if is_dma_read:
            dma_read_counter += 1
        else:
            dma_write_counter += 1

        disk_delta = disk_delta_dict.get(dma_disk_chunk, None)
        if disk_delta:
            # first search at overlay disk
            if disk_delta.ref_id != DeltaItem.REF_OVERLAY_MEM:
                msg = "dma disk chunk is same, but is it not deduped with overlay mem(%d)"\
                    % (disk_delta.ref_id)
                #LOG.info(msg)
                continue
            delta_mem_chunk = disk_delta.data/Const.CHUNK_SIZE
            if delta_mem_chunk == dma_mem_chunk:
                if is_dma_read:
                    dma_read_overlay_dedup += 1
                else:
                    dma_write_overlay_dedup += 1
        else:
            # search at overlay mem
            mem_delta = mem_delta_dict.get(dma_mem_chunk, None)
            if mem_delta:
                if mem_delta.ref_id != DeltaItem.REF_BASE_DISK:
                    msg = "dma memory chunk is same, but is it not deduped with base disk(%d)"\
                        % (mem_delta.ref_id)
                    #LOG.info(msg)
                    continue
                delta_disk_chunk = mem_delta.data/Const.CHUNK_SIZE
                if delta_disk_chunk == dma_disk_chunk:
                    if is_dma_read:
                        dma_read_base_dedup += 1
                    else:
                        dma_write_base_dedup += 1

    dma_end_time = time()
    LOG.debug("[DMA] Total DMA: %ld\n " % (len(dma_dict)))
    LOG.debug("[DMA] Total DMA READ: %ld, WRITE: %ld\n " %
              (dma_read_counter, dma_write_counter))
    LOG.debug("[DMA] WASTED TIME: %f\n " % (dma_end_time-dma_start_time))
    LOG.debug("[DMA] 1) DMA READ Overlay Deduplication: %ld(%f %%)\n " %
              (dma_read_overlay_dedup, 100.0 * dma_read_overlay_dedup / dma_read_counter))
    LOG.debug("[DMA]    DMA READ Base Deduplication: %ld(%f %%)\n " %
              (dma_read_base_dedup, 100.0 * dma_read_base_dedup / dma_read_counter))
    LOG.debug(
        "[DMA] 2) DMA WRITE Overlay Deduplication: %ld(%f %%)\n " %
        (dma_write_overlay_dedup, 100.0 * dma_write_overlay_dedup / dma_write_counter))
    LOG.debug(
        "[DMA]    DMA WRITE Base Deduplication: %ld(%f %%)\n " %
        (dma_write_base_dedup, 100.0 * dma_write_base_dedup / dma_write_counter))


def _convert_xml(disk_path, xml=None, mem_snapshot=None,
                 qemu_logfile=None, qmp_channel=None,
                 qemu_args=None, memory_snapshot_mode="suspend",
                 nova_xml=None, title=None, operation=None,
                 cpus=None, mem=None,fwd_ports=None):
    """process libvirt xml header before launching the VM
    :param memory_snapshot_mode : behavior of libvrt memory snapshotting, [off|suspend|live]
    """
    try:
        if mem_snapshot is not None:
            hdr = memory_util._QemuMemoryHeader(open(mem_snapshot))
            xml = ElementTree.fromstring(hdr.xml)
        else: 
            xml = ElementTree.fromstring(open(Const.TEMPLATE_XML, "r").read())
        original_xml_backup = ElementTree.tostring(xml)

        vm_name = None
        uuid = None
        nova_vnc_element = None
        uuid = uuid4()
        uuid_element = xml.find('uuid')
        old_uuid = uuid_element.text
        uuid_element.text = str(uuid)
        vm_name = 'nephele-' + str(uuid.hex)
        name_element = xml.find('name')
        if name_element is None:
            msg = "Malfomed XML input: %s", Const.TEMPLATE_XML
            raise CloudletGenerationError(msg)
        name_element.text = vm_name
        if title is not None:
            xml.find('title').text = title
        else:
            xml.find('title').text = vm_name
        if operation is not None:
            xml.find('description').text = '*NEPHELE managed* - %s' % operation

        # TODO: perhaps we shouldn't coerce the machine type because of incompatibility?
        #type = xml.find('os/type')
        #type.attrib['machine'] = 'pc-1.0'

        # enforce CPU model to qemu64 only if not specified
        # svm should not be added since it is not supported in x86 (QEMU bug)
        cpu_element = xml.find("cpu")
        if cpu_element is None:
            cpu_element = Element("cpu")
            xml.append(cpu_element)
        if cpu_element.find("model") is not None:
            cpu_element.remove(cpu_element.find("model"))
        if cpu_element.find("arch") is not None:
            cpu_element.remove(cpu_element.find("arch"))
        cpu_model_element = Element("model")
        cpu_model_element.text = "core2duo"
        cpu_model_element.set("fallback", "forbid")
        cpu_element.append(cpu_model_element)

        if cpus is not None:
            if cpus > 0:
                # overwrite vcpu node in template
                xml.find('vcpu').text = str(cpus)
                xml.find('cpu/topology').attrib['cores'] = str(cpus)
        if mem is not None:
            if mem > 0:
                # overwrite memory/currentMemory nodes in template
                xml.find('memory').text = str(mem)
                xml.find('currentMemory').text = str(mem)

        # update sysinfo entry's uuid if it exist
        # it has to match with uuid of the VM
        sysinfo_entries = xml.findall('sysinfo/system/entry')
        for entry in sysinfo_entries:
            if(entry.attrib['name'] == 'uuid'):
                entry.text = str(uuid)

        # update vnc information
        if nova_vnc_element is not None:
            device_element = xml.find("devices")
            graphics_elements = device_element.findall("graphics")
            for graphics_element in graphics_elements:
                if graphics_element.get("type") == "vnc":
                    device_element.remove(graphics_element)
                    device_element.append(nova_vnc_element)

        # Use custom QEMU
        qemu_emulator = xml.find('devices/emulator')
        if qemu_emulator is None:
            qemu_emulator = Element("emulator")
            device_element = xml.find("devices")
            device_element.append(qemu_emulator)
        qemu_emulator.text = Const.QEMU_BIN_PATH

        # find all disk element(hdd, cdrom) and change them to new
        disk_elements = xml.findall('devices/disk')
        hdd_source = None
        cdrom_source = None
        for disk_element in disk_elements:
            disk_type = disk_element.attrib['device']
            if disk_type == 'disk':
                hdd_source = disk_element.find('source')
                hdd_driver = disk_element.find("driver")
                if (hdd_driver is not None) and \
                        (hdd_driver.get("type", None) is not None):
                    hdd_driver.set("type", "raw")
            if disk_type == 'cdrom':
                cdrom_source = disk_element.find('source')
        if hdd_source is None:  # hdd path setting
            msg = "Malfomed XML input: %s", Const.TEMPLATE_XML
            raise CloudletGenerationError(msg)
        hdd_source.set("file", os.path.abspath(disk_path))
        if cdrom_source is not None:    # ovf path setting
            cdrom_source.set("file", os.path.abspath(Const.TEMPLATE_OVF))

        # append custom QEMU-argument
        qemu_xmlns = "http://libvirt.org/schemas/domain/qemu/1.0"
        qemu_element = xml.find("{%s}commandline" % qemu_xmlns)
        if qemu_element is None:
            qemu_element = Element("{%s}commandline" % qemu_xmlns)
            xml.append(qemu_element)

        existing_ports = False
        # remove previous custom argument, if it exists
        argument_list = qemu_element.findall("{%s}arg" % qemu_xmlns)
        remove_list = list()
        for argument_item in argument_list:
            arg_value = argument_item.get('value').strip()
            if arg_value.startswith('-cloudlet') or\
                    arg_value.startswith('logfile=') or\
                    arg_value.startswith('raw='):
                remove_list.append(argument_item)
            if arg_value.startswith('-redir'):
                existing_ports = True
        for item in remove_list:
            qemu_element.remove(item)
        # append custom qemu argument
        key_str = '-cloudlet'
        value_str = "raw=%s" % memory_snapshot_mode
        if qemu_logfile:
            value_str += ",logfile=%s" % qemu_logfile
        qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value': key_str}))
        qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value': value_str}))

        # remove previous qmp argument, if it exists
        argument_list = qemu_element.findall("{%s}arg" % qemu_xmlns)
        remove_list = list()
        for argument_item in argument_list:
            arg_value = argument_item.get('value').strip()
            if arg_value.startswith('-qmp') or arg_value.startswith('unix:'):
                remove_list.append(argument_item)
        for item in remove_list:
            qemu_element.remove(item)
        # append QMP channel for controlling live migration
        if qmp_channel is not None:
            qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value': '-qmp'}))
            qemu_element.append(
                Element(
                    "{%s}arg" % qemu_xmlns, {'value': "unix:%s,server,nowait" % qmp_channel}
                )
            )

        if fwd_ports is not None and not existing_ports:
            for f in fwd_ports:
                qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value': '-redir'}))
                qemu_element.append(
                    Element(
                        "{%s}arg" % qemu_xmlns, {'value': "tcp:%s::%s" % (f,f)}
                    )
                )


        # append qemu argument given from user
        if qemu_args:
            qemu_xmlns = "http://libvirt.org/schemas/domain/qemu/1.0"
            qemu_element = xml.find("{%s}commandline" % qemu_xmlns)
            if qemu_element is None:
                qemu_element = Element("{%s}commandline" % qemu_xmlns)
                xml.append(qemu_element)
            for each_argument in qemu_args:
                qemu_element.append(
                    Element("{%s}arg" % qemu_xmlns, {'value': each_argument}))

        # TODO: Handle console/serial element properly
        device_element = xml.find("devices")
        console_elements = device_element.findall("console")
        for console_element in console_elements:
            device_element.remove(console_element)
        serial_elements = device_element.findall("serial")
        for serial_element in serial_elements:
            device_element.remove(serial_element)

        network_element = device_element.find("interface")
        if network_element is not None:
            network_filter = network_element.find("filterref")
            if network_filter is not None:
                network_element.remove(network_filter)

        # remove security option: this option only works with OpenStack and
        # causes error in standalone version
        security_element = xml.find("seclabel")
        if security_element is not None:
            xml.remove(security_element)
        new_security_label = Element("seclabel")
        new_security_label.set("type", "dynamic")
        new_security_label.set("relabel", "yes")
        xml.append(new_security_label)

        new_xml_str = ElementTree.tostring(xml)
        new_xml_str = new_xml_str.replace(old_uuid, str(uuid))
        if mem_snapshot is not None:
            overwrite_xml(mem_snapshot, new_xml_str)
    except Exception as e:
        print e
    return original_xml_backup, new_xml_str


def _get_overlay_monitoring_info(conn, machine, options,
                                 base_memmeta, base_diskmeta,
                                 fuse_stream_monitor,
                                 base_disk, base_mem,
                                 modified_disk, modified_mem,
                                 qemu_logfile, nova_util=None):
    """return montioring information including
    1) base vm hash list
    2) used/freed disk block list
    3) freed memory page
    """

    if not options.DISK_ONLY:
        save_mem_snapshot(conn, machine, modified_mem, nova_util=nova_util,
                          fuse_stream_monitor=fuse_stream_monitor)

    # get hashlist of base memory and disk
    basemem_hashlist = memory.base_hashlist(base_memmeta)
    basedisk_hashlist = disk.base_hashlist(base_diskmeta)

    # get dma & discard information
    if options.TRIM_SUPPORT:
        dma_dict, trim_dict = disk.parse_qemu_log(
            qemu_logfile, Const.CHUNK_SIZE)
        if len(trim_dict) == 0:
            LOG.warning("No TRIM Discard, Check /etc/fstab configuration")
    else:
        trim_dict = dict()
    free_memory_dict = dict()

    # get used sector information from x-ray
    used_blocks_dict = None
    if options.XRAY_SUPPORT:
        import xray
        used_blocks_dict = xray.get_used_blocks(modified_disk)

    info_dict = dict()
    info_dict[OverlayMonitoringInfo.BASEDISK_HASHLIST] = basedisk_hashlist
    info_dict[OverlayMonitoringInfo.BASEMEM_HASHLIST] = basemem_hashlist
    info_dict[OverlayMonitoringInfo.DISK_USED_BLOCKS] = used_blocks_dict
    info_dict[OverlayMonitoringInfo.DISK_MODIFIED_BLOCKS] =\
        fuse_stream_monitor.modified_chunk_dict
    info_dict[OverlayMonitoringInfo.DISK_FREE_BLOCKS] = trim_dict
    info_dict[OverlayMonitoringInfo.MEMORY_FREE_BLOCKS] = free_memory_dict
    monitoring_info = OverlayMonitoringInfo(info_dict)
    return monitoring_info


def copy_disk(in_path, out_path):
    LOG.info("Copying disk image to %s" % out_path)
    cmd = ["cp",  "%s" % (in_path), "%s" % (out_path)]
    cp_proc = subprocess.Popen(cmd, close_fds=True)
    cp_proc.wait()
    if cp_proc.returncode != 0:
        raise IOError("Copy failed: from %s to %s " % (in_path, out_path))


def get_libvirt_connection():
    conn = libvirt.open("qemu:///system")
    return conn


def get_overlay_deltalist(monitoring_info, options,
                          base_image, base_mem,
                          base_memmeta, modified_disk,
                          modified_mem, old_deltalist=None):
    """return overlay deltalist
    Get difference between base vm (base_image, base_mem) and
    launch vm (modified_disk, modified_mem) using monitoring information
    """

    INFO = OverlayMonitoringInfo
    basedisk_hashlist = getattr(monitoring_info, INFO.BASEDISK_HASHLIST, None)
    basemem_hashlist = getattr(monitoring_info, INFO.BASEMEM_HASHLIST, None)
    free_memory_dict = getattr(monitoring_info, INFO.MEMORY_FREE_BLOCKS, None)
    m_chunk_dict = getattr(monitoring_info, INFO.DISK_MODIFIED_BLOCKS, None)
    trim_dict = getattr(monitoring_info, INFO.DISK_FREE_BLOCKS, None)
    used_blocks_dict = getattr(monitoring_info, INFO.DISK_USED_BLOCKS, None)
    dma_dict = dict()

    LOG.info("Get memory delta")
    if options.DISK_ONLY:
        mem_deltalist = list()
    else:
        mem_deltalist = memory.create_memory_deltalist(
            modified_mem,
            basemem_meta=base_memmeta,
            basemem_path=base_mem,
            apply_free_memory=options.FREE_SUPPORT,
            free_memory_info=free_memory_dict)
        if old_deltalist and len(old_deltalist) > 0:
            diff_deltalist = delta.residue_diff_deltalists(
                old_deltalist, mem_deltalist, base_mem)
            mem_deltalist = diff_deltalist

    LOG.info("Get disk delta")
    disk_statistics = dict()
    disk_deltalist = disk.create_disk_deltalist(
        modified_disk,
        m_chunk_dict,
        Const.CHUNK_SIZE,
        basedisk_hashlist=basedisk_hashlist,
        basedisk_path=base_image,
        trim_dict=trim_dict,
        apply_discard=True,
        dma_dict=dma_dict,
        used_blocks_dict=used_blocks_dict,
        ret_statistics=disk_statistics)
    LOG.info("Generate VM overlay using deduplication")
    merged_deltalist = delta.create_overlay(
        mem_deltalist, memory.Memory.RAM_PAGE_SIZE,
        disk_deltalist, Const.CHUNK_SIZE,
        basedisk_hashlist=basedisk_hashlist,
        basemem_hashlist=basemem_hashlist)
    LOG.info("Print statistics")
    free_memory_dict = getattr(
        monitoring_info,
        OverlayMonitoringInfo.MEMORY_FREE_BLOCKS,
        None)
    free_pfn_counter = long(free_memory_dict.get("freed_counter", 0))
    disk_discarded_count = disk_statistics.get('trimed', 0)
    DeltaList.statistics(merged_deltalist,
                         mem_discarded=free_pfn_counter,
                         disk_discarded=disk_discarded_count)

    return merged_deltalist


def _create_overlay_meta(overlay_metafile, base_hash, modified_disksize,
                         modified_memsize, blob_info):
    fout = open(overlay_metafile, "wb")

    meta_dict = dict()
    meta_dict[Const.META_BASE_VM_SHA256] = base_hash
    meta_dict[Const.META_RESUME_VM_DISK_SIZE] = long(modified_disksize)
    meta_dict[Const.META_RESUME_VM_MEMORY_SIZE] = long(modified_memsize)
    meta_dict[Const.META_OVERLAY_FILES] = blob_info

    serialized = msgpack.packb(meta_dict)
    fout.write(serialized)
    fout.close()


def generate_overlayfile(overlay_deltalist,
                         options,
                         base_hashvalue,
                         launchdisk_size,
                         launchmem_size,
                         overlay_metapath,
                         overlayfile_prefix):
    ''' generate overlay metafile and file
    :return: [overlay_metapath, [overlayfilepath1, overlayfilepath2]]
    '''

    # Compression
    LOG.info("[LZMA] Compressing overlay blobs (%s)", overlay_metapath)
    blob_list = delta.divide_blobs(
        overlay_deltalist,
        overlayfile_prefix,
        Const.OVERLAY_BLOB_SIZE_KB,
        Const.CHUNK_SIZE,
        memory.Memory.RAM_PAGE_SIZE)

    # create metadata
    if not options.DISK_ONLY:
        _create_overlay_meta(overlay_metapath, base_hashvalue,
                             launchdisk_size, launchmem_size, blob_list)
    else:
        _create_overlay_meta(overlay_metapath, base_hashvalue,
                             launchdisk_size, launchmem_size, blob_list)

    overlay_files = [item[Const.META_OVERLAY_FILE_NAME] for item in blob_list]
    dirpath = os.path.dirname(overlayfile_prefix)
    overlay_files = [os.path.join(dirpath, item) for item in overlay_files]

    return overlay_metapath, overlay_files


def recover_launchVM(base_image, meta_info, overlay_file, **kwargs):
    base_mem = kwargs.get('base_mem', None)
    base_diskmeta = kwargs.get('base_diskmeta', None)
    base_memmeta = kwargs.get('base_memmeta', None)

    if (not base_mem) or (not base_diskmeta) or (not base_memmeta):
        (base_diskmeta, base_mem, base_memmeta) = \
            Const.get_basepath(base_image, check_exist=True)
    launch_mem = NamedTemporaryFile(
        prefix="cloudlet-launch-mem-", delete=False)
    launch_disk = NamedTemporaryFile(
        prefix="cloudlet-launch-disk-", delete=False)

    # Get modified list from overlay_meta
    vm_disk_size = meta_info[Const.META_RESUME_VM_DISK_SIZE]
    vm_memory_size = meta_info[Const.META_RESUME_VM_MEMORY_SIZE]
    memory_chunks_all = set()
    disk_chunks_all = set()
    for each_file in meta_info[Const.META_OVERLAY_FILES]:
        memory_chunks = each_file[Const.META_OVERLAY_FILE_MEMORY_CHUNKS]
        disk_chunks = each_file[Const.META_OVERLAY_FILE_DISK_CHUNKS]
        memory_chunks_all.update(memory_chunks)
        disk_chunks_all.update(disk_chunks)

    # make FUSE disk & memory
    kwargs['meta_info'] = meta_info
    time_start_fuse = time()
    fuse = run_fuse(
        Const.CLOUDLETFS_PATH, Const.CHUNK_SIZE,
        base_image, vm_disk_size, base_mem, vm_memory_size,
        resumed_disk=launch_disk.name, disk_chunks=disk_chunks_all,
        resumed_memory=launch_mem.name, memory_chunks=memory_chunks_all,
        **kwargs
    )
    LOG.info("Start FUSE (%f s)" % (time()-time_start_fuse))
    LOG.debug("Overlay has %ld chunks" %
        (len(memory_chunks_all) + len(disk_chunks_all)))

    # Recover Modified Memory
    named_pipename = overlay_file+".fifo"
    os.mkfifo(named_pipename)

    delta_proc = delta.Recovered_delta(base_image, base_mem, overlay_file,
                                       launch_mem.name, vm_memory_size,
                                       launch_disk.name, vm_disk_size,
                                       Const.CHUNK_SIZE,
                                       out_pipename=named_pipename)

    fuse_thread = cloudletfs.FuseFeedingProc(
        fuse,
        named_pipename,
        delta.Recovered_delta.END_OF_PIPE)
    return [launch_disk.name, launch_mem.name, fuse, delta_proc, fuse_thread]


def run_fuse(bin_path, chunk_size, original_disk, fuse_disk_size,
             original_memory, fuse_memory_size,
             resumed_disk=None, disk_chunks=None, disk_overlay_map=None,
             resumed_memory=None, memory_chunks=None, memory_overlay_map=None,
             valid_bit=0, **kwargs):
    if fuse_disk_size <= 0:
        raise CloudletGenerationError("FUSE disk size should be bigger than 0")
    if original_memory is not None and fuse_memory_size <= 0:
        raise CloudletGenerationError(
            "FUSE memory size should be bigger than 0")

    # run fuse file system
    original_disk = os.path.abspath(original_disk)
    original_memory = os.path.abspath(original_memory)
    resumed_disk = os.path.abspath(resumed_disk) if resumed_disk else ""
    resumed_memory = os.path.abspath(resumed_memory) if resumed_memory else ""

    if disk_overlay_map is None:
        disk_overlay_map = ','.join("%ld:%d" % (item, valid_bit)
                                     for item in disk_chunks or [])
    if memory_overlay_map is None:
        memory_overlay_map = ','.join("%ld:%d" % (item, valid_bit)
                                      for item in memory_chunks or [])

    # launch fuse
    execute_args = [
        # disk parameter
        original_disk.replace('\n', ''),    # base path
        resumed_disk.replace('\n', ''),     # overlay path
        disk_overlay_map,                   # overlay map
        '%d' % fuse_disk_size,              # size of base
        "%d" % chunk_size,
    ]
    if original_memory:
        execute_args.extend([
            # memory parameter
            original_memory.replace('\n', ''),
            resumed_memory.replace('\n', ''),
            memory_overlay_map,
            '%d' % fuse_memory_size,
            "%d" % chunk_size,
        ])

    # if the overlay is already populated we don't have to pass the list of
    # locally missing chunks to cloudletfs.
    if valid_bit == 1:
        disk_chunks = memory_chunks = None

    fuse_process = cloudletfs.CloudletFS(
        bin_path, execute_args,
        modified_disk_chunks=disk_chunks,
        modified_memory_chunks=memory_chunks,
        **kwargs)
    fuse_process.launch()
    fuse_process.start()
    return fuse_process


def launch_vm(conn, domain_xml):
    machine = conn.createXML(domain_xml, 0)
    return machine

def run_vm(conn, domain_xml, **kwargs):
    machine = conn.createXML(domain_xml, 0)

    # Run VNC and wait until user finishes working
    if kwargs.get('vnc_disable'):
        return machine

    # Get VNC port
    vnc_port = 5900
    try:
        running_xml_string = machine.XMLDesc(libvirt.VIR_DOMAIN_XML_SECURE)
        running_xml = ElementTree.fromstring(running_xml_string)
        vnc_port = running_xml.find("devices/graphics").get("port")
        vnc_port = int(vnc_port)-5900
    except AttributeError as e:
        LOG.error("Warning, Possible VNC port error:%s" % str(e))

    _PIPE = subprocess.PIPE
    vnc_process = subprocess.Popen(["gvncviewer", "localhost:%d" % vnc_port],
                                   stdout=_PIPE, stderr=_PIPE,
                                   close_fds=True)
    if kwargs.get('wait_vnc'):
        try:
            vnc_process.wait()
        except KeyboardInterrupt as e:
            LOG.info("keyboard interrupt while waiting VNC")
            if machine:
                machine.destroy()
    return machine


class QmpThreadSerial(native_threading.Thread):

    def __init__(self, qmp_path, fuse_stream_monitor):
        self.qmp_path = qmp_path
        self.stop = native_threading.Event()
        self.qmp = qmp_af_unix.QmpAfUnix(self.qmp_path)
        self.fuse_stream_monitor = fuse_stream_monitor
        native_threading.Thread.__init__(self, target=self.stop_migration)

    def stop_migration(self):
        self.qmp.connect()
        sleep(2)    # wait until qemu is ready read negotiation command
        counter_check_comp_size = 0
        ret = self.qmp.qmp_negotiate()
        if not ret:
            raise CloudletGenerationError("failed to connect to qmp channel")
        LOG.debug("waiting to start live migration before stopping it")
        self.migration_stop_time = self._stop_migration()
        LOG.debug("Finish sending stop migration")
        self.qmp.disconnect()

    def _stop_migration(self):
        stop_time = self.qmp.stop_raw_live()
        self.fuse_stream_monitor.terminate()
        return stop_time

    def terminate(self):
        self.stop.set()


class MemoryReadThread(native_threading.Thread):
    RET_SUCCESS = 1
    RET_ERROR = 2

    def __init__(self, input_path, output_path, machine_memory_size):
        self.input_path = input_path
        self.output_path = output_path
        self.ret = self.RET_ERROR
        self.machine_memory_size = machine_memory_size*1024
        native_threading.Thread.__init__(self, target=self.read_mem_snapshot)

    def read_mem_snapshot(self):
        retry_counter = 0
        while os.path.exists(self.input_path) == False:
            retry_counter += 1
            sleep(1)
            if retry_counter > 10:
                self.ret = self.RET_ERROR
                return

        # create memory snapshot aligned with 4KB
        try:
            self.in_fd = open(self.input_path, 'rb')
            self.out_fd = open(self.output_path, 'wb')

            total_read_size = 0
            # read first 40KB and aligen header with 4KB
            data = self.in_fd.read(memory.Memory.RAM_PAGE_SIZE*10)
            libvirt_header = memory_util._QemuMemoryHeaderData(data)
            original_header = libvirt_header.get_header()
            align_size = memory.Memory.RAM_PAGE_SIZE*2
            new_header = libvirt_header.get_aligned_header(align_size)
            self.out_fd.write(new_header)
            total_read_size += len(new_header)
            self.out_fd.write(data[len(original_header):])
            total_read_size += len(data[len(original_header):])
            LOG.info("Header size of memory snapshot is %s" % len(new_header))

            # write rest of the memory data
            prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)
            while True:
                data = self.in_fd.read(1024 * 10)
                if data is None or len(data) <= 0:
                    break
                self.out_fd.write(data)
                total_read_size += len(data)
                prog_bar.set_percent(
                    100.0 * total_read_size / self.machine_memory_size)
                prog_bar.show_progress()
            self.out_fd.flush()
            prog_bar.finish()
            if total_read_size != self.out_fd.tell():
                msg = "output file size is different from stream size"
                raise Exception(msg)

        except Exception as e:
            LOG.error(str(e))
            self.ret = self.RET_ERROR
        else:
            self.ret = self.RET_SUCCESS


def save_mem_snapshot(conn, machine, fout_path, **kwargs):
    # Set migration speed
    nova_util = kwargs.get('nova_util', None)
    fuse_stream_monitor = kwargs.get('fuse_stream_monitor', None)
    ret = machine.migrateSetMaxSpeed(1000000, 0)   # 1000 Gbps, unlimited
    if ret != 0:
        msg = "Cannot set migration speed : %s", machine.name()
        raise CloudletGenerationError(msg)

    # Stop monitoring for memory access (snapshot will create a lot of access)
    if fuse_stream_monitor:
        fuse_stream_monitor.del_path(cloudletfs.StreamMonitor.MEMORY_ACCESS)

    # get VM information
    machine_memory_size = machine.memoryStats().get('actual', None)
    if machine_memory_size is None:
        # libvirt <= 0.9.3
        xml = ElementTree.fromstring(
            machine.XMLDesc(libvirt.VIR_DOMAIN_XML_SECURE))
        memory_element = xml.find('memory')
        if memory_element is not None:
            machine_memory_size = long(memory_element.text)

    # Save memory state
    LOG.info("save VM memory state")
    try:
        fifo_path = NamedTemporaryFile(prefix="cloudlet-memory-snapshot-",
                                       delete=True)
        named_pipe_output = fifo_path.name + ".fifo"
        if os.path.exists(named_pipe_output):
            os.remove(named_pipe_output)
        os.mkfifo(named_pipe_output)
        if nova_util:
            # OpenStack runs VM with nova account and snapshot
            # is generated from system connection
            nova_util.chown(named_pipe_output, os.getuid())
        memory_read_thread = MemoryReadThread(named_pipe_output,
                                              fout_path,
                                              machine_memory_size)
        memory_read_thread.start()
        LOG.debug("start machine save")
        machine.save(named_pipe_output)  # green thread blocked in here
        LOG.debug("finish machine save")
    except libvirt.libvirtError as e:
        # we intentionally ignore a couple of libvirt exceptions
        if str(e).startswith('unable to seek') or \
                str(e).startswith('internal error: migration was active, but no RAM info was set'):
            pass
        else:
            raise CloudletGenerationError("libvirt memory save : " + str(e))
    finally:
        if os.path.exists(named_pipe_output):
            os.remove(named_pipe_output)

    try:
        ret = memory_read_thread.ret
        if ret != MemoryReadThread.RET_SUCCESS:
            msg = "Failed to create memory snapshot"
            LOG.error(msg)
            raise CloudletGenerationError(msg)
        if nova_util is not None:
            # OpenStack runs VM with nova account and snapshot
            # is generated from system connection
            nova_util.chown(fout_path, os.getuid())
    except memory_util.MachineGenerationError as e:
        raise CloudletGenerationError("Machine Generation Error: " + str(e))


def run_snapshot(conn, disk_image, mem_snapshot,
                 new_xml_string, resume_time=None):
    if resume_time is not None:
        start_resume_time = time()

    # resume
    restore_with_config(conn, mem_snapshot, new_xml_string)
    if resume_time is not None:
        resume_time['start_time'] = start_resume_time
        resume_time['end_time'] = time()
        LOG.info("[RESUME] : QEMU resume time (%f)~(%f)=(%f)" %
                (resume_time['start_time'], resume_time['end_time'],
                 resume_time['end_time']-resume_time['start_time']))

    # get machine
    domxml = ElementTree.fromstring(new_xml_string)
    uuid_element = domxml.find('uuid')
    uuid = str(uuid_element.text)
    machine = conn.lookupByUUIDString(uuid)

    return machine


def connect_vnc(machine, no_wait=False):
    # Get VNC port
    vnc_port = 5900
    vnc_ip = '127.0.0.1'
    try:
        running_xml_string = machine.XMLDesc(libvirt.VIR_DOMAIN_XML_SECURE)
        running_xml = ElementTree.fromstring(running_xml_string)
        vnc_port = running_xml.find("devices/graphics").get("port")
        vnc_ip = running_xml.find("devices/graphics").get("listen")
        vnc_port = int(vnc_port)-5900
    except AttributeError as e:
        LOG.error("Warning, Possible VNC port error:%s" % str(e))

    # Run VNC
    cmd = ["gvncviewer", "%s:%d" % (vnc_ip, vnc_port)]
    vnc_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    if no_wait:
        return

    LOG.info("waiting for finishing VNC interaction")
    try:
        vnc_process.wait()
    except KeyboardInterrupt as e:
        LOG.info("keyboard interrupt while waiting VNC")
        vnc_process.terminate()


def rettach_nic(machine, old_xml, new_xml, **kwargs):
    """rettach NIC of the VM
    """
    old_xml = ElementTree.fromstring(old_xml)
    old_nic = old_xml.find('devices/interface')
    filter_element = old_nic.find("filterref")
    if filter_element is not None:
        old_nic.remove(filter_element)
    old_nic_xml = ElementTree.tostring(old_nic)
    ret = machine.detachDevice(old_nic_xml)
    if ret != 0:
        LOG.warning("failed to detach device")
    sleep(2)

    # attach
    new_xml = ElementTree.fromstring(new_xml)
    new_nic = new_xml.find('devices/interface')
    filter_element = new_nic.find("filterref")
    if filter_element is not None:
        new_nic.remove(filter_element)
    new_nic_xml = ElementTree.tostring(new_nic)

    retry_count = 1
    while retry_count <= 3:
        try:
            ret = machine.attachDevice(new_nic_xml)
            if ret != 0:
                LOG.warning(
                    "failed to attach device (trying..%d)" % retry_count)
            LOG.info("success to rettach nic device")
            break
        except libvirt.libvirtError as e:
            LOG.info("Failed to rettach NIC")
            LOG.info(str(e))
            sleep(2)
        retry_count += 1


def restore_with_config(conn, mem_snapshot, xml):
    try:
        LOG.info("restoring VM...")
        conn.restoreFlags(mem_snapshot, xml, libvirt.VIR_DOMAIN_SAVE_RUNNING)
        LOG.info("VM is restored...")
    except libvirt.libvirtError as e:
        message = "Libvirt error when restoring snapshot: %s\n" % e.message
        raise CloudletGenerationError(message)


def overwrite_xml(in_path, new_xml):
    fin = open(in_path, "rb")
    hdr = memory_util._QemuMemoryHeader(fin)
    fin.close()

    # Write header
    fin = open(in_path, "r+b")
    hdr.overwrite(fin, new_xml)
    fin.close()


def copy_with_xml(in_path, out_path, xml):
    fin = open(in_path)
    fout = open(out_path, 'r+b')
    hdr = memory_util._QemuMemoryHeader(fin)

    # Write header
    hdr.xml = xml
    hdr.write(fout)
    fout.flush()

    # move to the content
    hdr.seek_body(fin)
    fout.write(fin.read())


def synthesis_statistics(meta_info, decomp_overlay_file,
                         mem_access_list, disk_access_list):
    start_time = time()

    delta_list = DeltaList.fromfile(decomp_overlay_file)
    total_overlay_size = os.path.getsize(decomp_overlay_file)
    delta_dic = dict()
    for delta_item in delta_list:
        delta_dic[delta_item.index] = delta_item

    overlay_mem_chunks = dict()
    overlay_disk_chunks = dict()
    access_per_blobs = dict()
    total_overlay_mem_chunks = 0
    total_overlay_disk_chunks = 0

    # get all overlay chunks from meta info
    for each_file in meta_info[Const.META_OVERLAY_FILES]:
        memory_chunks = each_file[Const.META_OVERLAY_FILE_MEMORY_CHUNKS]
        disk_chunks = each_file[Const.META_OVERLAY_FILE_DISK_CHUNKS]
        blob_name = each_file[Const.META_OVERLAY_FILE_NAME]
        for mem_chunk in memory_chunks:
            index = DeltaItem.get_index(
                DeltaItem.DELTA_MEMORY, mem_chunk * memory.Memory.RAM_PAGE_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            overlay_mem_chunks[mem_chunk] = {
                "blob_name": blob_name, 'chunk_size': chunk_size}
        for disk_chunk in disk_chunks:
            index = DeltaItem.get_index(
                DeltaItem.DELTA_DISK, disk_chunk * Const.CHUNK_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            overlay_disk_chunks[disk_chunk] = {
                "blob_name": blob_name, 'chunk_size': chunk_size}
        # (memory, memory_total, disk, disk_total)
        access_per_blobs[blob_name] = {
            'mem_access': 0,
            'mem_access_size': 0,
            'mem_total': len(memory_chunks),
            'disk_access': 0,
            'disk_access_size': 0,
            'disk_total': len(disk_chunks),
            'blob_size': each_file[Const.META_OVERLAY_FILE_SIZE]}
        total_overlay_mem_chunks += len(memory_chunks)
        total_overlay_disk_chunks += len(disk_chunks)

    # compare real accessed chunks with overlay chunk list
    overlay_mem_access_count = 0
    overlay_disk_access_count = 0
    overlay_mem_access_size = 0
    overlay_disk_access_size = 0
    for access_chunk in mem_access_list:
        if overlay_mem_chunks.get(access_chunk, None) is not None:
            index = DeltaItem.get_index(
                DeltaItem.DELTA_MEMORY,
                access_chunk *
                memory.Memory.RAM_PAGE_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            blob_name = overlay_mem_chunks.get(access_chunk)['blob_name']
            chunk_size = overlay_mem_chunks.get(access_chunk)['chunk_size']
            access_per_blobs[blob_name]['mem_access'] += 1  # 0: memory
            access_per_blobs[blob_name]['mem_access_size'] += chunk_size
            overlay_mem_access_count += 1
            overlay_mem_access_size += chunk_size
    for access_chunk in disk_access_list:
        if overlay_disk_chunks.get(access_chunk, None) is not None:
            index = DeltaItem.get_index(
                DeltaItem.DELTA_DISK,
                access_chunk *
                Const.CHUNK_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            blob_name = overlay_disk_chunks.get(access_chunk)['blob_name']
            chunk_size = overlay_disk_chunks.get(access_chunk)['chunk_size']
            access_per_blobs[blob_name]['disk_access'] += 1
            access_per_blobs[blob_name]['disk_access_size'] += chunk_size
            overlay_disk_access_count += 1
            overlay_disk_access_size += chunk_size

    LOG.debug("-------------------------------------------------")
    LOG.debug("## Synthesis Statistics (took %f seconds) ##" %
        (time()-start_time))
    LOG.debug("Overlay acccess count / total overlay count\t: %d / %d = %05.2f %%" %
              (overlay_mem_access_count + overlay_disk_access_count,
               total_overlay_mem_chunks + total_overlay_disk_chunks, 
               100.0 * (overlay_mem_access_count + overlay_disk_access_count) /
                  (total_overlay_mem_chunks + total_overlay_disk_chunks)))
    LOG.debug("Overlay acccess size / total overlay size\t: %10.3d MB/ %10.3f MB= %05.2f %%" %
              ((overlay_mem_access_size+overlay_disk_access_size)/1024.0/1024,
               (total_overlay_size/1024.0/1024),
               100.0 * (overlay_mem_access_size+overlay_disk_access_size)/total_overlay_size))
    try:
        LOG.debug(
            "  Memory Count: Overlay memory acccess / total memory overlay\t: %d / %d = %05.2f %%" %
            (overlay_mem_access_count, total_overlay_mem_chunks,
             100.0 * overlay_mem_access_count / total_overlay_mem_chunks))
        LOG.debug(
            "  Memory Size: Overlay memory acccess / total overlay\t: %d / %d = %05.2f %%" %
            (overlay_mem_access_size, total_overlay_size,
             100.0 * overlay_mem_access_size / total_overlay_size))
        LOG.debug(
            "  Disk Count: Overlay acccess / total disk overlay\t: %d / %d = %05.2f %%" %
            (overlay_disk_access_count, total_overlay_disk_chunks,
             100.0 * overlay_disk_access_count / total_overlay_disk_chunks))
        LOG.debug(
            "  Disk Size: Overlay acccess / total overlay\t: %d / %d = %05.2f %%" %
            (overlay_disk_access_size, total_overlay_size,
             100.0 * overlay_disk_access_size / total_overlay_size))
        LOG.debug(
            "  EXTRA (count): Overlay memory acccess / VM memory access\t: %d / %d = %05.2f %%" %
            (overlay_mem_access_count,
             len(mem_access_list),
             100.0 * overlay_mem_access_count / len(mem_access_list)))
        LOG.debug(
            "  EXTRA (count): Overlay disk acccess / VM disk access\t: %d / %d = %05.2f %%" %
            (overlay_disk_access_count, len(disk_access_list),
             100.0 * overlay_disk_access_count / len(disk_access_list)))
    except ZeroDivisionError as e:
        pass
    used_blob_count = 0
    used_blob_size = 0
    for blob_name in access_per_blobs.keys():
        mem_access = access_per_blobs[blob_name]['mem_access']
        mem_access_size = access_per_blobs[blob_name]['mem_access_size']
        total_mem_chunks = access_per_blobs[blob_name]['mem_total']
        disk_access = access_per_blobs[blob_name]['disk_access']
        disk_access_size = access_per_blobs[blob_name]['disk_access_size']
        total_disk_chunks = access_per_blobs[blob_name]['disk_total']
        if mem_access > 0:
            used_blob_count += 1
            used_blob_size += access_per_blobs[blob_name]['blob_size']
        if total_mem_chunks != 0:
            pass
        '''
            LOG.debug("    %s\t:\t%d/%d\t=\t%5.2f is used (%d bytes uncompressed)" % \
                    (blob_name, mem_access+disk_access, \
                    total_mem_chunks+total_disk_chunks, \
                    (mem_access+disk_access)*100.0/(total_mem_chunks+total_disk_chunks),
                    (mem_access_size+disk_access_size)))
                    '''
    LOG.debug("%d blobs (%f MB) are required out of %d (%05.2f %%)" %
              (used_blob_count, used_blob_size / 1024.0 / 1024,
               len(access_per_blobs.keys()),
               used_blob_count * 100.0 / len(access_per_blobs.keys())))
    LOG.debug("-------------------------------------------------")


# share with OpenStack
def _create_baseVM(conn, domain,
                   base_diskpath, base_mempath, base_diskmeta, base_memmeta,
                   **kwargs):
    """generate base vm given base_diskpath
    """
    # make memory snapshot
    # VM has to be paused first to perform stable disk hashing
    save_mem_snapshot(conn, domain, base_mempath, **kwargs)
    LOG.info("Start Base VM Memory hashing")
    base_mem = memory.hashing(base_mempath)
    base_mem.export_to_file(base_memmeta)
    LOG.info("Finish Base VM Memory hashing")

    # generate disk hashing
    # TODO: need more efficient implementation, e.g. bisect
    LOG.info("Start Base VM Disk hashing")
    base_hashvalue = disk.hashing(base_diskpath, base_diskmeta)
    LOG.info("Finish Base VM Disk hashing")
    return base_hashvalue


"""External API Start
"""


def validate_congifuration():
    return True


def validate_handoffurl(handoff_url):
    parsed_handoff_url = urlsplit(handoff_url)
    if parsed_handoff_url.scheme != "file" and parsed_handoff_url.scheme != "tcp":
        return False
    if parsed_handoff_url.scheme == "tcp":
        # expect to have destination address for network transmission case
        if len(parsed_handoff_url.netloc) == 0:
            return False
    return True


def create_baseVM(disk_image_path, source=None, title=None, cpus=None, mem=None):
    # Create Base VM(disk, memory) snapshot using given VM disk image
    # :param disk_image_path : file path of the VM disk image
    # :returns: (generated base VM disk path, generated base VM memory path)

    # Check DB
    disk_image_path = os.path.abspath(disk_image_path)
    (base_diskmeta, base_mempath, base_memmeta) = \
        Const.get_basepath(disk_image_path)

    # check sanity
    if not os.path.exists(Const.TEMPLATE_XML):
        raise CloudletGenerationError("Cannot find Base VM default XML at %s\n"
                                      % Const.TEMPLATE_XML)
    if not os.path.exists(Const.TEMPLATE_OVF):
        raise CloudletGenerationError("Cannot find ovf file for AMIt %s\n"
                                      % Const.TEMPLATE_OVF)

    # allow write permission to base disk and delete all previous files
    #os.chmod(disk_image_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    if os.path.exists(base_diskmeta):
        os.unlink(base_diskmeta)
    if os.path.exists(base_mempath):
        os.unlink(base_mempath)
    if os.path.exists(base_memmeta):
        os.unlink(base_memmeta)

    # edit default XML to have new disk path
    conn = get_libvirt_connection()
    xml, new_xml_string = _convert_xml(disk_path=disk_image_path, title=title, operation='Creating Base VM Image', cpus=cpus, mem=mem)
    
    # launch VM & wait for end of vnc
    machine = None
    try:
        print 'Launching VM...\nPause VM when finished modifying to begin hashing of disk/memory state.' 
        machine = launch_vm(conn, new_xml_string)
        #wait until VM is paused
        while True:
            state, _ = machine.state()
            if state == libvirt.VIR_DOMAIN_PAUSED:
                break

        base_hashvalue = _create_baseVM(conn,
                                        machine,
                                        disk_image_path,
                                        base_mempath,
                                        base_diskmeta,
                                        base_memmeta)
    except Exception as e:
        LOG.error("failed at %s" % str(traceback.format_exc()))
        sys.exit(1)
    finally:
        if machine is not None:
            _terminate_vm(conn, machine)
            machine = None
        

    # save the result to DB
    dbconn = db_api.DBConnector()
    basevm_list = dbconn.list_item(db_table.BaseVM)
    for item in basevm_list:
        if disk_image_path == item.disk_path:
            dbconn.del_item(item)
            break
    new_basevm = db_table.BaseVM(disk_image_path, base_hashvalue, source)
    dbconn.add_item(new_basevm)

    # write hashvalue to file
    hashfile_path = Const.get_base_hashpath(disk_image_path)
    open(hashfile_path, "w+").write(str(base_hashvalue) + "\n")

    return disk_image_path, base_mempath

def handlesig(signum, frame):
    global HANDOFF_SIGNAL_RECEIVED
    LOG.info("Received signal(%d) to start handoff..." % signum)
    HANDOFF_SIGNAL_RECEIVED = True

def increment_filename(path):
    #strip file:// prefix
    if path.startswith('file://'):
        path = path[7:]
    path      = os.path.expanduser(path)
    if not os.path.exists(path):
        return path

    root, ext = os.path.splitext(path)
    if root.rfind('_') != -1:
        root = root[:root.rfind('_')]
    dir       = os.path.dirname(root)
    fname     = os.path.basename(root)
    candidate = fname+ext
    index     = 1
    ls        = set(os.listdir(dir))
    while candidate in ls:
            candidate = "{}_{}{}".format(fname,index,ext)
            index    += 1
    return os.path.join(dir,candidate)

def synthesize(base_disk, overlay_path, **kwargs):
    """VM Synthesis and run recoverd VM
    :param base_disk: path to base disk
    :param overlay_path: path to VM overlay file
    :param kwargs-disk_only: synthesis size VM with only disk image
    :param kwargs-handoff_url: return residue of changed portion
    """
    global HANDOFF_SIGNAL_RECEIVED
    LOG.debug("==========================================")
    LOG.debug(overlay_path)

    disk_only = kwargs.get('disk_only', False)
    zip_container = kwargs.get('zip_container', False)
    handoff_url = kwargs.get('handoff_url', None)
    qemu_args = kwargs.get('qemu_args', False)
    overlay_mode = kwargs.get('overlay_mode', None)
    is_profiling_test = kwargs.get('is_profiling_test', False)
    xml = kwargs.get('xml', None)
    base_mem = kwargs.get('base_mem', None)
    base_diskmeta = kwargs.get('base_diskmeta', None)
    base_memmeta = kwargs.get('base_memmeta', None)
    save_snapshot = False
    title = kwargs.get('title', None)
    fwd_ports = kwargs.get('fwd_ports', None)

    overlay_filename = NamedTemporaryFile(prefix="cloudlet-overlay-file-")
    decompe_time_s = time()
    if not zip_container:
        LOG.info("Decompressing VM overlay")
        meta_info = compression.decomp_overlay(overlay_path,
                                               overlay_filename.name)
    else:
        meta_info = compression.decomp_overlayzip(overlay_path,
                                                  overlay_filename.name)

    base_sha = meta_info[Const.META_BASE_VM_SHA256]
    base_found = False
    dbconn = db_api.DBConnector()
    basevm_list = dbconn.list_item(db_table.BaseVM)
    for basevm_row in basevm_list:
        if basevm_row.hash_value == base_sha:
            base_disk = os.path.abspath(basevm_row.disk_path)
            base_found = True
    if not base_found:
        msg = "Cannot find base image (SHA-256: %s) referenced in overlay: %s" % (base_sha, overlay_path)
        raise CloudletGenerationError(msg)

    LOG.info("Decompression time : %f (s)" % (time()-decompe_time_s))
    LOG.info("Recovering launch VM")
    launch_disk, launch_mem, fuse, delta_proc, fuse_thread = \
        recover_launchVM(base_disk, meta_info, overlay_filename.name, **kwargs)
    # resume VM
    LOG.info("Resume the launch VM")
    synthesized_VM = SynthesizedVM(
        launch_disk, launch_mem, fuse, disk_only=disk_only,
        qemu_args=qemu_args, title=title, fwd_ports=fwd_ports
    )
    # no-pipelining
    delta_proc.start()
    fuse_thread.start()
    delta_proc.join()
    fuse_thread.join()
    machine = synthesized_VM.resume()

    # preload basevm hash dictionary for creating residue
    (base_diskmeta, base_mem, base_memmeta) =\
        Const.get_basepath(base_disk, check_exist=False)
    preload_thread = handoff.PreloadResidueData(
        base_diskmeta, base_memmeta)
    preload_thread.daemon = True
    preload_thread.start()
    signal.signal(signal.SIGUSR1, handlesig)
    while True:
        state, _ = machine.state()
        if state == libvirt.VIR_DOMAIN_PAUSED:
            #make a new snapshot and store it
            handoff_url = 'file://%s' % (increment_filename(overlay_path)) 
            save_snapshot = True
            print 'VM entered paused state. Generating snapshot of disk and memory...'
            op_id = log_op(op=Const.OP_BUILD_SNAPSHOT,notes="Image: %s, Dest: %s" % (base_sha, overlay_path))
            break
        elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
            #disambiguate between reboot and shutoff
            sleep(1)
            try:
                if machine.isActive():
                    state, _ = machine.state()
                    if state == libvirt.VIR_DOMAIN_RUNNING:
                        continue
                    else:
                        print "VM has been powered off. Tearing down FUSE..."
                        synthesized_VM.terminate()
                        return
                else:
                    print "VM is no longer running. Tearing down FUSE..."
                    synthesized_VM.terminate()
                    return
            except libvirt.libvirtError as e:
                synthesized_VM.terminate()
                return
        elif HANDOFF_SIGNAL_RECEIVED == True:
            #read destination from file
            fdest = open('/tmp/%s.cloudlet-handoff' % os.getpid(), "rb")
            meta = msgpack.unpackb(fdest.read())
            fdest.close()
            #validate that the meta data is really for us
            if meta['pid'] == os.getpid():
                handoff_url = meta['url']
                print 'Handoff initiated for %s to the following destination: %s' % (meta['title'], meta['url'])
                op_id = log_op(op=Const.OP_HANDOFF,notes="Title: %s, PID: %d, Dest: %s" % (meta['title'], meta['pid'], handoff_url))
                HANDOFF_SIGNAL_RECEIVED = False
                os.remove(HANDOFF_TEMP)
                break
            else:
                print 'PID in %s does not match getpid!' % HANDOFF_TEMP



    options = Options()
    options.TRIM_SUPPORT = True
    options.FREE_SUPPORT = True
    options.DISK_ONLY = False
    preload_thread.join()
    (base_diskmeta, base_mem, base_memmeta) = \
        Const.get_basepath(base_disk, check_exist=False)
    base_vm_paths = [base_disk, base_mem, base_diskmeta, base_memmeta]
    # prepare data structure for VM handoff
    residue_zipfile = None
    dest_handoff_url = handoff_url
    parsed_handoff_url = urlsplit(handoff_url)
    if parsed_handoff_url.scheme == "file":
        dest_path = parsed_handoff_url.path
        if os.path.exists(os.path.dirname(dest_path)):
            dest_handoff_url = "file://%s" % os.path.abspath(parsed_handoff_url.path)
        else:
            print "Destination doesn't exist, attempting to use temporary file..."
            temp_dir = mkdtemp(prefix="cloudlet-residue-")
            residue_zipfile = os.path.join(temp_dir, Const.OVERLAY_ZIP)
            dest_handoff_url = "file://%s" % os.path.abspath(residue_zipfile)
    handoff_ds = handoff.HandoffDataSend()
    LOG.debug("save data to file")
    handoff_ds.save_data(
        base_vm_paths, meta_info[Const.META_BASE_VM_SHA256],
        preload_thread.basedisk_hashdict,
        preload_thread.basemem_hashdict,
        options, dest_handoff_url, overlay_mode,
        synthesized_VM.fuse.mountpoint, synthesized_VM.qemu_logfile,
        synthesized_VM.qmp_channel, synthesized_VM.machine.ID(),
        synthesized_VM.fuse.modified_disk_chunks, "qemu:///system",
        title, fwd_ports
    )
    handoff_ds._load_vm_data()
    try:
        handoff.perform_handoff(handoff_ds)
    except handoff.HandoffError as e:
        LOG.error("Cannot perform VM handoff: %s" % (str(e)))
    except Exception as e:
        LOG.error("Unhandled exception: %s", e)
    # print out residue location
    if residue_zipfile and os.path.exists(residue_zipfile):
        LOG.info("Save new VM overlay at: %s" % (os.path.abspath(residue_zipfile)))
    if save_snapshot:
        dbconn = DBConnector()
        list = dbconn.list_item(table_def.Snapshot)
        for item in list:
            if os.path.abspath(parsed_handoff_url.path) == item.path:
                dbconn.del_item(item)
                break
        _, basevm = PackagingUtil._get_matching_basevm(disk_path=base_disk)
        new = table_def.Snapshot(os.path.abspath(parsed_handoff_url.path), basevm.hash_value)
        dbconn.add_item(new)

    update_op(op_id, has_ended=True)
    # terminate
    synthesized_VM.monitor.terminate()
    synthesized_VM.monitor.join()
    synthesized_VM.terminate()

def info_vm_overlay(overlay_path):
    overlay_path = os.path.abspath(overlay_path)
    if os.path.exists(overlay_path) == False:
        msg = "VM overlay does not exist at %s" % overlay_path
        raise IOError(msg)
    overlay_package = VMOverlayPackage("file://%s" % overlay_path)
    meta_raw = overlay_package.read_meta()
    meta_info = msgpack.unpackb(meta_raw)
    baseVMsha256 = meta_info[Const.META_BASE_VM_SHA256]
    vm_disk_size = meta_info[Const.META_RESUME_VM_DISK_SIZE]
    vm_memory_size = meta_info[Const.META_RESUME_VM_MEMORY_SIZE]

    modified_disk_chunk_count = 0
    modified_memory_chunk_count = 0
    comp_overlay_files = meta_info[Const.META_OVERLAY_FILES]
    comp_overlay_files = [item for item in comp_overlay_files]
    for comp_filename in comp_overlay_files:
        modified_disk_chunk_count += len(
            item[Const.META_OVERLAY_FILE_DISK_CHUNKS])
        modified_memory_chunk_count += len(
            item[Const.META_OVERLAY_FILE_MEMORY_CHUNKS])
    output = "VM overlay\t\t\t: %s\n" % overlay_path
    output += "Base VM ID\t\t\t: %s\n" % baseVMsha256
    output += "# of modified disk chunk\t: %s\n" % modified_disk_chunk_count
    output += "# of modified memory chunk\t: %s\n" % modified_memory_chunk_count
    output += "VM disk size\t\t\t: %s bytes\n" % vm_disk_size
    output += "VM memory size\t\t\t: %s bytes\n" % vm_memory_size
    return output


'''External API End
'''
