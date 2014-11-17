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
import Memory
import Disk
import cloudletfs
import memory_util
import delta
import hashlib
import libvirt
import shutil
import multiprocessing
import json

from db import api as db_api
from db import table_def as db_table
from Configuration import Const
from Configuration import Options
from Configuration import VMOverlayCreationMode
from Configuration import Caching_Const
from delta import DeltaList
from delta import DeltaItem
import msgpack
from progressbar import AnimatedProgressBar
from package import VMOverlayPackage
import process_manager

from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from uuid import uuid4
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from time import time
from time import sleep
import threading
import traceback
from optparse import OptionParser

from tool import comp_lzma
from tool import diff_files
import compression
import log as logging

LOG = logging.getLogger(__name__)


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
        except Exception, e:
            kwargs.update(dict(zip(function.func_code.co_varnames[2:], args)))
            LOG.error("failed : reasons - %s, args - %s" % (str(e), str(kwargs)))
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


class VM_Overlay(threading.Thread):
    def __init__(self, base_disk, options, qemu_args=None,
            base_mem=None, base_diskmeta=None,
            base_memmeta=None, base_hashvalue=None,
            nova_xml=None, nova_util=None, nova_conn=None):
        # create user customized overlay.
        # First resume VM, then let user edit its VM
        # Finally, return disk/memory binary as an overlay
        # base_disk: path to base disk
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
        if self.base_hashvalue == None:
            dbconn = db_api.DBConnector()
            basevm_list = dbconn.list_item(db_table.BaseVM)
            for basevm_row in basevm_list:
                basevm_row_disk = os.path.abspath(basevm_row.disk_path)
                if basevm_row_disk == self.base_disk:
                    self.base_hashvalue = basevm_row.hash_value
            if self.base_hashvalue == None:
                msg = "Cannot find hashvalue for %s" % self.base_disk
                raise CloudletGenerationError(msg)

        self.log_path = os.path.join(os.path.dirname(self.base_disk), \
                                os.path.basename(self.base_disk) + Const.OVERLAY_LOG)
        threading.Thread.__init__(self, target=self.create_overlay)

    @wrap_vm_fault
    def resume_basevm(self):
        if (self.options == None) or (isinstance(self.options, Options) == False):
            raise CloudletGenerationError("Given option class is invalid: %s" % str(self.options))

        # filename for overlay VM
        temp_qemu_dir = mkdtemp(prefix="cloudlet-qemu-")
        self.qemu_logfile = os.path.join(temp_qemu_dir, "qemu-trim-log")
        open(self.qemu_logfile, "w+").close()
        os.chmod(os.path.dirname(self.qemu_logfile), 0o771)
        os.chmod(self.qemu_logfile, 0o666)

        # option for data-intensive application
        self.cache_manager = None
        self.mount_point = None
        if self.options.DATA_SOURCE_URI != None:
            cache_manager, mount_point = self._start_emulate_cache_fs()

        # make FUSE disk & memory
        self.fuse = run_fuse(Const.CLOUDLETFS_PATH, Const.CHUNK_SIZE,
                self.base_disk, os.path.getsize(self.base_disk),
                self.base_mem, os.path.getsize(self.base_mem))
        self.modified_disk = os.path.join(self.fuse.mountpoint, 'disk', 'image')
        self.base_mem_fuse = os.path.join(self.fuse.mountpoint, 'memory', 'image')
        self.modified_mem = NamedTemporaryFile(prefix="cloudlet-mem-", delete=False)
        # monitor modified chunks
        stream_modified = os.path.join(self.fuse.mountpoint, 'disk', 'streams', 'chunks_modified')
        stream_access = os.path.join(self.fuse.mountpoint, 'disk', 'streams', 'chunks_accessed')
        memory_access = os.path.join(self.fuse.mountpoint, 'memory', 'streams', 'chunks_accessed')
        self.fuse_stream_monitor = cloudletfs.StreamMonitor()
        self.fuse_stream_monitor.add_path(stream_modified, cloudletfs.StreamMonitor.DISK_MODIFY)
        self.fuse_stream_monitor.add_path(stream_access, cloudletfs.StreamMonitor.DISK_ACCESS)
        self.fuse_stream_monitor.add_path(memory_access, cloudletfs.StreamMonitor.MEMORY_ACCESS)
        self.fuse_stream_monitor.start()
        self.qemu_monitor = cloudletfs.FileMonitor(self.qemu_logfile, cloudletfs.FileMonitor.QEMU_LOG)
        self.qemu_monitor.start()

        # 1. resume & get modified disk
        LOG.info("* Overlay creation configuration")
        LOG.info("  - %s" % str(self.options))
        self.old_xml_str, self.new_xml_str = _convert_xml(self.modified_disk, \
                mem_snapshot=self.base_mem_fuse, \
                qemu_logfile=self.qemu_logfile, \
                qemu_args=self.qemu_args, \
                nova_xml=self.nova_xml)
        self.machine = run_snapshot(self.conn, self.modified_disk, 
                self.base_mem_fuse, self.new_xml_str)
        return self.machine

    @wrap_vm_fault
    def create_overlay(self):
        # 2. get montoring info
        monitoring_info = _get_monitoring_info(self.conn, self.machine, 
                self.options,
                self.base_memmeta, self.base_diskmeta,
                self.fuse_stream_monitor,
                self.base_disk, self.base_mem,
                self.modified_disk, self.modified_mem.name,
                self.qemu_logfile, 
                nova_util=self.nova_util)

        # 3. get overlay VM
        overlay_deltalist = get_overlay_deltalist(monitoring_info, self.options,
                                                  self.base_disk, self.base_mem, 
                                                  self.base_diskmeta, self.base_memmeta, 
                                                  self.modified_disk, self.modified_mem.name)

        # 4. create_overlayfile
        temp_dir = mkdtemp(prefix="cloudlet-overlay-")
        overlay_prefix = os.path.join(temp_dir, Const.OVERLAY_FILE_PREFIX)
        overlay_metapath = os.path.join(temp_dir, Const.OVERLAY_META)

        self.overlay_files, overlay_info = _generate_overlayfile(overlay_deltalist, overlay_prefix)
        self.overlay_metafile = _generate_overlaymeta(overlay_metapath, overlay_info,
                                                      self.base_hashvalue,
                                                      os.path.getsize(self.modified_disk),
                                                      os.path.getsize(self.modified_mem.name))

        # packaging VM overlay into a single zip file
        if self.options.ZIP_CONTAINER == True:
            self.overlay_zipfile = os.path.join(temp_dir, Const.OVERLAY_ZIP)
            VMOverlayPackage.create(self.overlay_zipfile, self.overlay_metafile, self.overlay_files)
            # delete tmp overlay files
            if os.path.exists(self.overlay_metafile) == True:
                os.remove(self.overlay_metafile)
            for overlay_file in self.overlay_files:
                if os.path.exists(overlay_file) == True:
                    os.remove(overlay_file)

        # 5. terminate
        # option for data-intensive application
        if self.options.DATA_SOURCE_URI != None:
            overlay_uri_meta = os.path.join(temp_dir, Const.OVERLAY_URIs)
            self.overlay_uri_meta =  self._terminate_emulate_cache_fs(overlay_uri_meta)
        self.terminate()

    def terminate(self):
        if hasattr(self, 'fuse_stream_monitor') and self.fuse_stream_monitor != None:
            self.fuse_stream_monitor.terminate()
            self.fuse_stream_monitor.join()
        if hasattr(self, 'fuse') and self.fuse != None:
            self.fuse.terminate()
            self.fuse.join()
        if hasattr(self, 'qemu_monitor') and self.qemu_monitor != None:
            self.qemu_monitor.terminate()
            self.qemu_monitor.join()
        if hasattr(self, "cache_manager") and self.cache_manager != None:
            self.cache_manager.terminate()
            self.cache_manager.join()
        if hasattr(self, "mount_point") and self.mount_point != None and os.path.lexists(self.mount_point):
            os.unlink(self.mount_point)
        if self.options.MEMORY_SAVE_PATH:
            LOG.debug("moving memory sansphost to %s" % self.options.MEMORY_SAVE_PATH)
            shutil.move(self.modified_mem.name, self.options.MEMORY_SAVE_PATH)
        else:
            os.unlink(self.modified_mem.name)

        # delete cloudlet-qemu-log directory
        if os.path.exists(os.path.dirname(self.qemu_logfile)):
            shutil.rmtree(os.path.dirname(self.qemu_logfile))

    def exception_handler(self):
        # make sure to destory the VM
        self.terminate()
        if hasattr(self, 'machine'):
            _terminate_vm(self.conn, self.machine)
            self.machine = None

    def _start_emulate_cache_fs(self):
        # check samba
        if os.path.exists(Caching_Const.HOST_SAMBA_DIR) == False:
            msg = "Cloudlet does not have samba directory at %s\n" % \
                    Caching_Const.HOST_SAMBA_DIR
            msg += "You can change samba path at Configuration.py\n"
            raise CloudletGenerationError(msg)
        # fetch URI to cache
        try:
            cache_manager = cache.CacheManager(Caching_Const.CACHE_ROOT, \
                    Caching_Const.REDIS_ADDR, Caching_Const.CACHE_FUSE_BINPATH)
                    
            cache_manager.start()
            self.compiled_list = cache.Util.get_compiled_URIs( \
                    cache_manager.cache_dir, self.options.DATA_SOURCE_URI)
            #cache_manager.fetch_compiled_URIs(compiled_list)
            cache_fuse = cache_manager.launch_fuse(self.compiled_list)
            LOG.debug("cache fuse mount : %s, %s\n" % \
                    (cache_fuse.url_root, cache_fuse.mountpoint))
        except cache.CachingError, e:
            msg = "Cannot retrieve data from URI: %s" % str(e)
            raise CloudletGenerationError(msg)
        # create symbolic link to samba dir
        mount_point = os.path.join(Caching_Const.HOST_SAMBA_DIR, cache_fuse.url_root)
        if os.path.lexists(mount_point) == True:
            os.unlink(mount_point)
        os.symlink(cache_fuse.mountpoint, mount_point)
        LOG.debug("create symbolic link to %s" % mount_point)

        return cache_manager, cache_fuse.mountpoint

    def _terminate_emulate_cache_fs(self, overlay_uri_meta):
        uri_list = []
        for each_info in self.compiled_list:
            uri_list.append(each_info.get_uri())
        uri_data = {
                'source_URI' : self.options.DATA_SOURCE_URI,
                'compiled_URIs' : uri_list,
                }
        with open(overlay_uri_meta, "w+b") as f:
            import json
            f.write(json.dumps(uri_data))
        return overlay_uri_meta


class _MonitoringInfo(object):
    BASEMEM_HASHDICT        = "basemem_hash_dict"
    DISK_MODIFIED_BLOCKS    = "disk_modified_block" # from fuse monitoring
    DISK_USED_BLOCKS        = "disk_used_block" # from xray support
    DISK_FREE_BLOCKS        = "disk_free_block"
    MEMORY_FREE_BLOCKS      = "memory_free_block"

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


class SynthesizedVM(threading.Thread):
    def __init__(self, launch_disk, launch_mem, fuse, disk_only=False, qemu_args=None, **kwargs):
        # kwargs
        self.nova_xml= kwargs.get("nova_xml", None)
        self.conn = kwargs.get("nova_conn", None) or get_libvirt_connection()
        self.LOG = kwargs.get("log", None)
        if self.LOG == None:
            self.LOG = open("/dev/null", "w+b")

        # monitor modified chunks
        self.machine = None
        self.disk_only = disk_only
        self.qemu_args = qemu_args
        self.fuse = fuse
        self.launch_disk = launch_disk
        self.launch_mem = launch_mem

        temp_qemu_dir = mkdtemp(prefix="cloudlet-overlay-")
        self.qemu_logfile = os.path.join(temp_qemu_dir, "qemu-trim-log")
        open(self.qemu_logfile, "w+").close()
        os.chmod(os.path.dirname(self.qemu_logfile), 0o771)
        os.chmod(self.qemu_logfile, 0o666)
        LOG.info("Launch disk: %s" % os.path.abspath(launch_disk))
        LOG.info("Launch memory: %s" % os.path.abspath(launch_mem))

        self.resumed_disk = os.path.join(fuse.mountpoint, 'disk', 'image')
        self.resumed_mem = os.path.join(fuse.mountpoint, 'memory', 'image')
        self.stream_modified = os.path.join(fuse.mountpoint, 'disk', 'streams', 'chunks_modified')
        self.stream_disk_access = os.path.join(fuse.mountpoint, 'disk', 'streams', 'chunks_accessed')
        self.stream_memory_access = os.path.join(fuse.mountpoint, 'memory', 'streams', 'chunks_accessed')
        self.monitor = cloudletfs.StreamMonitor()
        self.monitor.add_path(self.stream_modified, cloudletfs.StreamMonitor.DISK_MODIFY)
        self.monitor.add_path(self.stream_disk_access, cloudletfs.StreamMonitor.DISK_ACCESS)
        self.monitor.add_path(self.stream_memory_access, cloudletfs.StreamMonitor.MEMORY_ACCESS)
        self.monitor.start()
        self.qemu_monitor = cloudletfs.FileMonitor(self.qemu_logfile, cloudletfs.FileMonitor.QEMU_LOG)
        self.qemu_monitor.start()

        threading.Thread.__init__(self, target=self.resume)

    def _generate_xml(self):
        # convert xml
        if self.disk_only:
            xml = ElementTree.fromstring(open(Const.TEMPLATE_XML, "r").read())
            self.old_xml_str, self.new_xml_str = _convert_xml(self.resumed_disk,
                    xml=xml, qemu_logfile=self.qemu_logfile,
                    qemu_args=self.qemu_args)
        else:
            self.old_xml_str, self.new_xml_str = _convert_xml(self.resumed_disk,
                    mem_snapshot=self.resumed_mem,
                    qemu_logfile=self.qemu_logfile,
                    qemu_args=self.qemu_args,
                    nova_xml=self.nova_xml)

    def resume(self):
        #resume VM
        self.resume_time = {'time':-100}
        self._generate_xml()
        try:
            if self.disk_only:
                # edit default XML to have new disk path
                self.machine = run_vm(self.conn, self.new_xml_string, 
                        vnc_disable=True)
            else:
                self.machine = run_snapshot(self.conn, self.resumed_disk, 
                        self.resumed_mem, self.new_xml_str, 
                        resume_time=self.resume_time)
        except Exception as e:
            sys.stdout.write(str(e)+"\n")

        return self.machine

    def terminate(self):
        try:
            if hasattr(self, 'machine') == True and self.machine != None:
                self.machine.destroy()
        except libvirt.libvirtError as e:
            pass

        # terminate
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
    except libvirt.libvirtError, e:
        pass


def _update_overlay_meta(original_meta, new_path, blob_info=None):
    fout = open(new_path, "wrb")

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
    disk_delta_dict = dict([(delta.offset/Const.CHUNK_SIZE, delta) for delta in disk_deltalist])
    mem_delta_dict = dict([(delta.offset/Const.CHUNK_SIZE, delta) for delta in mem_deltalist])
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
#                LOG.info("dma disk chunk is same, but is it not deduped with overlay mem(%d)" \
#                        % (disk_delta.ref_id))
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
#                    LOG.info("dma memory chunk is same, but is it not deduped with base disk(%d)" \
#                            % (mem_delta.ref_id))
                    continue
                delta_disk_chunk = mem_delta.data/Const.CHUNK_SIZE
                if delta_disk_chunk == dma_disk_chunk:
                    if is_dma_read:
                        dma_read_base_dedup += 1
                    else:
                        dma_write_base_dedup += 1

    dma_end_time = time()
    LOG.debug("[DMA] Total DMA: %ld\n " % (len(dma_dict)))
    LOG.debug("[DMA] Total DMA READ: %ld, WRITE: %ld\n " % (dma_read_counter, dma_write_counter))
    LOG.debug("[DMA] WASTED TIME: %f\n " % (dma_end_time-dma_start_time))
    LOG.debug("[DMA] 1) DMA READ Overlay Deduplication: %ld(%f %%)\n " % \
            (dma_read_overlay_dedup, 100.0*dma_read_overlay_dedup/dma_read_counter))
    LOG.debug("[DMA]    DMA READ Base Deduplication: %ld(%f %%)\n " % \
            (dma_read_base_dedup, 100.0*dma_read_base_dedup/dma_read_counter))
    LOG.debug("[DMA] 2) DMA WRITE Overlay Deduplication: %ld(%f %%)\n " % \
            (dma_write_overlay_dedup, 100.0*dma_write_overlay_dedup/dma_write_counter))
    LOG.debug("[DMA]    DMA WRITE Base Deduplication: %ld(%f %%)\n " % \
            (dma_write_base_dedup, 100.0*dma_write_base_dedup/dma_write_counter))


def _convert_xml(disk_path, xml=None, mem_snapshot=None, \
        qemu_logfile=None, qemu_args=None, nova_xml=None):
    # we need either input xml or memory snapshot path
    # if we have mem_snapshot, we update new xml to the memory snapshot
    
    if xml == None and mem_snapshot == None:
        raise CloudletGenerationError("we need either input xml or memory snapshot path")

    if mem_snapshot != None:
        hdr = memory_util._QemuMemoryHeader(open(mem_snapshot))
        xml = ElementTree.fromstring(hdr.xml)
    original_xml_backup = ElementTree.tostring(xml)

    vm_name = None
    uuid = None
    nova_vnc_element = None
    if nova_xml != None:
        new_xml = ElementTree.fromstring(nova_xml)
        vm_name = str(new_xml.find('name').text)
        uuid = str(new_xml.find('uuid').text)
        nova_graphics_element = new_xml.find('devices/graphics')
        if (nova_graphics_element is not None) and \
                (nova_graphics_element.get('type') == 'vnc'):
            nova_vnc_element = nova_graphics_element

        #network_element = new_xml.find('devices/interface')
        #network_xml_str = ElementTree.tostring(network_element)

    # delete padding
    padding_element = xml.find("description")
    if padding_element != None:
        xml.remove(padding_element)

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
    # append 'disable svm flag' if not exist
    cpu_feature_elements = cpu_element.findall("feature")
    is_svm_feature = False
    for cpu_feature_element in cpu_feature_elements:
        feature_policy = cpu_feature_element.get("policy")
        feature_name = cpu_feature_element.get("name")
        if feature_policy.lower() == "disable" and\
                feature_name.lower() == "svm":
            is_svm_feature = True
    '''
    if is_svm_feature is False:
        cpu_feature_element = Element("feature")
        cpu_feature_element.set("policy", "disable")
        cpu_feature_element.set("name", "svm")
        cpu_element.append(cpu_feature_element)
    '''

    # update uuid
    if uuid is None:
        uuid = uuid4()
    uuid_element = xml.find('uuid')
    old_uuid = uuid_element.text
    uuid_element.text = str(uuid)
    # update sysinfo entry's uuid if it exist
    # it has to match with uuid of the VM
    sysinfo_entries = xml.findall('sysinfo/system/entry')
    for entry in sysinfo_entries:
        if(entry.attrib['name'] == 'uuid'):
            entry.text = str(uuid)

    # update vm_name
    if vm_name is None:
        vm_name = 'cloudlet-' + str(uuid.hex)
    name_element = xml.find('name')
    if name_element is None:
        msg = "Malfomed XML input: %s", Const.TEMPLATE_XML
        raise CloudletGenerationError(msg)
    name_element.text = vm_name

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
    # hdd path setting
    if hdd_source is None:
        msg = "Malfomed XML input: %s", Const.TEMPLATE_XML
        raise CloudletGenerationError(msg)
    hdd_source.set("file", os.path.abspath(disk_path))
    # ovf path setting
    if cdrom_source is not None:
        cdrom_source.set("file", os.path.abspath(Const.TEMPLATE_OVF))

    # append QEMU-argument
    if qemu_logfile is not None:
        qemu_xmlns="http://libvirt.org/schemas/domain/qemu/1.0"
        qemu_element = xml.find("{%s}commandline" % qemu_xmlns)
        if qemu_element is None:
            qemu_element = Element("{%s}commandline" % qemu_xmlns)
            xml.append(qemu_element)
        # remove previous cloudlet argument if it is
        argument_list = qemu_element.findall("{%s}arg" % qemu_xmlns)
        remove_list = list()
        for argument_item in argument_list:
            arg_value = argument_item.get('value').strip()
            if arg_value.startswith('-cloudlet') or arg_value.startswith('logfile='):
                remove_list.append(argument_item)
        for item in remove_list:
            qemu_element.remove(item)
        # append new cloudlet logpath
        qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value':'-cloudlet'}))
        qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value':"logfile=%s" % qemu_logfile}))

    # append qemu argument given from user
    if qemu_args:
        qemu_xmlns = "http://libvirt.org/schemas/domain/qemu/1.0"
        qemu_element = xml.find("{%s}commandline" % qemu_xmlns)
        if qemu_element is None:
            qemu_element = Element("{%s}commandline" % qemu_xmlns)
            xml.append(qemu_element)
        for each_argument in qemu_args:
            qemu_element.append(Element("{%s}arg" % qemu_xmlns, {'value':each_argument}))

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

    # remove security option:
    # this option only works with OpenStack and causes error in standalone version
    security_element = xml.find("seclabel")
    if security_element is not None:
        xml.remove(security_element)

    new_xml_str = ElementTree.tostring(xml)
    new_xml_str = new_xml_str.replace(old_uuid, str(uuid))
    if mem_snapshot is not None:
        overwrite_xml(mem_snapshot, new_xml_str)

    return original_xml_backup, new_xml_str


class VMMonitor(object):
    def __init__(self, conn, machine, options,
                 fuse_stream_monitor,
                 base_disk, base_mem,
                 modified_disk,
                 qemu_logfile, 
                 original_deltalist=None,
                 nova_util=None):
        self.conn = conn
        self.machine = machine
        self.options = options
        self.fuse_stream_monitor = fuse_stream_monitor
        self.base_disk = base_disk
        self.base_mem = base_mem
        self.modified_disk = modified_disk
        self.qemu_logfile = qemu_logfile
        self.original_deltalist = original_deltalist
        self.nova_util = nova_util
        self.memory_snapshot_size = -1
        #threading.Thread.__init__(self, target=self.load_monitoring_info)

    def get_monitoring_info(self):
        ''' return montioring information including
            1) base vm hash list
            2) used/freed disk block list
            3) freed memory page
        '''
        # puase the VM if it's not yet
        if self.machine is not None:
            vm_state, reason = self.machine.state(0)
            if vm_state != libvirt.VIR_DOMAIN_PAUSED:
                self.machine.suspend()

        # 2. get dma & discard information
        if self.options.TRIM_SUPPORT:
            dma_dict, trim_dict = Disk.parse_qemu_log(self.qemu_logfile, Const.CHUNK_SIZE)
            if len(trim_dict) == 0:
                LOG.warning("No TRIM Discard, Check /etc/fstab configuration")
        else:
            trim_dict = dict()
        free_memory_dict = dict()
        time_parse_trim_log = time()

        # 3. get used sector information from x-ray
        used_blocks_dict = None
        if self.options.XRAY_SUPPORT:
            import xray
            used_blocks_dict = xray.get_used_blocks(modified_disk)

        info_dict = dict()
        info_dict[_MonitoringInfo.DISK_USED_BLOCKS] = used_blocks_dict
        info_dict[_MonitoringInfo.DISK_FREE_BLOCKS] = trim_dict
        info_dict[_MonitoringInfo.MEMORY_FREE_BLOCKS] = free_memory_dict
        # mark the modifid disk area in the original VM overlay as modified area
        m_chunk_dict = dict()
        if self.original_deltalist is not None:
            for o_delta_item in self.original_deltalist:
                if o_delta_item.delta_type == DeltaItem.DELTA_DISK:
                    modified_index = o_delta_item.offset / Const.CHUNK_SIZE
                    m_chunk_dict[modified_index] = 1.0
        m_chunk_dict.update(self.fuse_stream_monitor.modified_chunk_dict)
        info_dict[_MonitoringInfo.DISK_MODIFIED_BLOCKS] = m_chunk_dict

        self.monitoring_info = _MonitoringInfo(info_dict)
        return self.monitoring_info


def copy_disk(in_path, out_path):
    LOG.info("Copying disk image to %s" % out_path)
    cmd = ["cp",  "%s" % (in_path), "%s" % (out_path)]
    cp_proc = subprocess.Popen(cmd, close_fds=True)
    cp_proc.wait()
    if cp_proc.returncode != 0:
        raise IOError("Copy failed: from %s to %s " % (in_path, out_path))


def get_libvirt_connection():
    conn = libvirt.open("qemu:///session")
    return conn


def get_overlay_deltalist(monitoring_info, options,
                          overlay_mode,
                          base_image, base_mem, 
                          base_memmeta, 
                          basedisk_hashdict, 
                          basemem_hashdict,
                          modified_disk,
                          modified_mem_queue,
                          merged_deltalist_queue,
                          process_controller):
    '''return overlay deltalist
    Get difference between base vm (base_image, base_mem) and 
    launch vm (modified_disk, modified_mem) using monitoring information

    Args:
        prev_mem_deltalist : Option only for creating_residue.
            Different from disk, we create whole memory snapshot even for residue.
            So, to get the precise difference between previous memory overlay,
            we need previous memory deltalist
    '''

    INFO = _MonitoringInfo
    free_memory_dict = getattr(monitoring_info, INFO.MEMORY_FREE_BLOCKS, None)
    m_chunk_dict = getattr(monitoring_info, INFO.DISK_MODIFIED_BLOCKS, dict())
    trim_dict = getattr(monitoring_info, INFO.DISK_FREE_BLOCKS, None)
    used_blocks_dict = getattr(monitoring_info, INFO.DISK_USED_BLOCKS, None)
    dma_dict = dict()
    apply_discard = True

    LOG.info("Get memory delta")
    time_s = time()

    # memory hashdict is neede at memory deltaand dedup
    if not options.DISK_ONLY:
        memory_deltalist_queue = multiprocessing.Queue(maxsize=overlay_mode.QUEUE_SIZE_MEMORY_DELTA_LIST)
        memory_deltalist_proc = Memory.CreateMemoryDeltalist(modified_mem_queue,
                                                             memory_deltalist_queue,
                                                             base_memmeta, base_mem,
                                                             overlay_mode,
                                                             options.FREE_SUPPORT,
                                                             free_memory_dict)
        memory_deltalist_proc.start()
    time_mem_delta = time()

    LOG.info("Get disk delta")
    disk_deltalist_queue = multiprocessing.Queue(maxsize=overlay_mode.QUEUE_SIZE_DISK_DELTA_LIST)
    disk_deltalist_proc = Disk.CreateDiskDeltalist(modified_disk,
                                                   m_chunk_dict,
                                                   Const.CHUNK_SIZE,
                                                   disk_deltalist_queue,
                                                   base_image,
                                                   overlay_mode,
                                                   trim_dict,
                                                   dma_dict,
                                                   apply_discard,
                                                   used_blocks_dict)
    disk_deltalist_proc.start()

    time_disk_delta = time()
    LOG.info("Generate VM overlay using deduplication")

    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "CreateMemoryDeltalist")
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "CreateDiskDeltalist")

    dedup_proc = delta.DeltaDedup(memory_deltalist_queue, Memory.Memory.RAM_PAGE_SIZE,
                                  disk_deltalist_queue, Const.CHUNK_SIZE,
                                  merged_deltalist_queue,
                                  overlay_mode.NUM_PROC_OPTIMIZATION,
                                  overlay_mode,
                                  basedisk_hashdict=basedisk_hashdict,
                                  basemem_hashdict=basemem_hashdict)
    dedup_proc.start()
    time_merge_delta = time()

    #LOG.info("Print statistics")
    #disk_deltalist_proc.join()   # to fill out disk_statistics
    #disk_statistics = disk_deltalist_proc.ret_statistics
    #free_memory_dict = getattr(monitoring_info, _MonitoringInfo.MEMORY_FREE_BLOCKS, None)
    #free_pfn_counter = long(free_memory_dict.get("freed_counter", 0))
    #disk_discarded_count = disk_statistics.get('trimed', 0)
    #DeltaList.statistics(merged_deltalist,
    #        mem_discarded=free_pfn_counter,
    #        disk_discarded=disk_discarded_count)
    time_e = time()
    LOG.debug("Total time for getting deltalist: %f" % (time_e-time_s))
    LOG.debug("  memory deltalist: %f" % (time_mem_delta-time_s))
    LOG.debug("  disk deltalist: %f" % (time_disk_delta-time_mem_delta))
    LOG.debug("  merge deltalist: %f" % (time_merge_delta-time_disk_delta))
    return dedup_proc


def _generate_overlaymeta(overlay_metapath, overlay_info, base_hashvalue,
                          launchdisk_size, launchmem_size):
    # create metadata
    fout = open(overlay_metapath, "wrb")

    meta_dict = dict()
    meta_dict[Const.META_BASE_VM_SHA256] = base_hashvalue
    meta_dict[Const.META_RESUME_VM_DISK_SIZE] = long(launchdisk_size)
    meta_dict[Const.META_RESUME_VM_MEMORY_SIZE] = long(launchmem_size)
    meta_dict[Const.META_OVERLAY_FILES] = overlay_info

    serialized = msgpack.packb(meta_dict)
    fout.write(serialized)
    fout.close()

    return overlay_metapath


def run_delta_compression(output_list, **kwargs):
    # kwargs
    # LOG = log object for nova
    # nova_util = nova_util is executioin wrapper for nova framework
    #           You should use nova_util in OpenStack, or subprocess
    #           will be returned without finishing their work
    # custom_delta
    log = kwargs.get('log', None)
    nova_util = kwargs.get('nova_util', None)
    custom_delta = kwargs.get('custom_delta', False)

    # xdelta and compression
    ret_files = []
    for (base, modified, overlay) in output_list:
        start_time = time()

        # xdelta
        if custom_delta:
            diff_files(base, modified, overlay, nova_util=nova_util)
        else:
            diff_files(base, modified, overlay, nova_util=nova_util)
        LOG.info('[TIME] time for creating overlay : ', str(time()-start_time))
        LOG.info('[TIME](%d)-(%d)=(%d): ' % (os.path.getsize(base), os.path.getsize(modified), os.path.getsize(overlay)))

        # compression
        comp = overlay + '.lzma'
        comp, time1 = comp_lzma(overlay, comp, nova_util=nova_util)
        ret_files.append(comp)

        # remove temporary files
        os.remove(modified)
        os.remove(overlay)

    return ret_files


def recover_launchVM(base_image, meta_info, overlay_file, **kwargs):
    # kwargs
    # skip_validation   :   skip sha1 validation
    # LOG = log object for nova
    # nova_util = nova_util is executioin wrapper for nova framework
    #           You should use nova_util in OpenStack, or subprocess
    #           will be returned without finishing their work
    base_mem = kwargs.get('base_mem', None)
    base_diskmeta = kwargs.get('base_diskmeta', None)
    base_memmeta = kwargs.get('base_memmeta', None)

    if (not base_mem) or (not base_diskmeta) or (not base_memmeta):
        (base_diskmeta, base_mem, base_memmeta) = \
                Const.get_basepath(base_image, check_exist=True)
    launch_mem = NamedTemporaryFile(prefix="cloudlet-launch-mem-", delete=False)
    launch_disk = NamedTemporaryFile(prefix="cloudlet-launch-disk-", delete=False)

    # Get modified list from overlay_meta
    vm_disk_size = meta_info[Const.META_RESUME_VM_DISK_SIZE]
    vm_memory_size = meta_info[Const.META_RESUME_VM_MEMORY_SIZE]
    memory_chunk_list = list()
    disk_chunk_list = list()
    for each_file in meta_info[Const.META_OVERLAY_FILES]:
        memory_chunks = each_file[Const.META_OVERLAY_FILE_MEMORY_CHUNKS]
        disk_chunks = each_file[Const.META_OVERLAY_FILE_DISK_CHUNKS]
        memory_chunk_list.extend(["%ld:0" % item for item in memory_chunks])
        disk_chunk_list.extend(["%ld:0" % item for item in disk_chunks])
    disk_overlay_map = ','.join(disk_chunk_list)
    memory_overlay_map = ','.join(memory_chunk_list)

    # make FUSE disk & memory
    kwargs['meta_info'] = meta_info
    fuse = run_fuse(Const.CLOUDLETFS_PATH, Const.CHUNK_SIZE,
            base_image, vm_disk_size, base_mem, vm_memory_size,
            resumed_disk=launch_disk.name,  disk_overlay_map=disk_overlay_map,
            resumed_memory=launch_mem.name, memory_overlay_map=memory_overlay_map,
            **kwargs)
    LOG.info("Start FUSE")

    # Recover Modified Memory
    named_pipename = overlay_file+".fifo"
    os.mkfifo(named_pipename)

    delta_proc = delta.Recovered_delta(base_image, base_mem, overlay_file, \
            launch_mem.name, vm_memory_size,
            launch_disk.name, vm_disk_size, Const.CHUNK_SIZE,
            out_pipename=named_pipename)
    fuse_thread = cloudletfs.FuseFeedingProc(fuse,
            named_pipename, delta.Recovered_delta.END_OF_PIPE)
    return [launch_disk.name, launch_mem.name, fuse, delta_proc, fuse_thread]


def run_fuse(bin_path, chunk_size, original_disk, fuse_disk_size,
        original_memory, fuse_memory_size,
        resumed_disk=None, disk_overlay_map=None,
        resumed_memory=None, memory_overlay_map=None,
        **kwargs):
    if fuse_disk_size <= 0:
        raise CloudletGenerationError("FUSE disk size should be bigger than 0")
    if original_memory != None and fuse_memory_size <= 0:
        raise CloudletGenerationError("FUSE memory size should be bigger than 0")

    # run fuse file system
    resumed_disk = os.path.abspath(resumed_disk) if resumed_disk else ""
    resumed_memory = os.path.abspath(resumed_memory) if resumed_memory else ""
    disk_overlay_map = str(disk_overlay_map) if disk_overlay_map else ""
    memory_overlay_map = str(memory_overlay_map) if memory_overlay_map else ""

    # launch fuse
    execute_args = [
            # disk parameter
            "%s" % os.path.abspath(original_disk),  # base path
            "%s" % resumed_disk,                    # overlay path
            "%s" % disk_overlay_map,                # overlay map
            '%d' % fuse_disk_size,                  # size of base
            "%d" % chunk_size]
    if original_memory:
        for parameter in [
                # memory parameter
                "%s" % os.path.abspath(original_memory),
                "%s" % resumed_memory,
                "%s" % memory_overlay_map,
                '%d' % fuse_memory_size,
                "%d" % chunk_size
                ]:
            execute_args.append(parameter)

    fuse_process = cloudletfs.CloudletFS(bin_path, execute_args, **kwargs)
    fuse_process.launch()
    fuse_process.start()
    return fuse_process


def run_vm(conn, domain_xml, **kwargs):
    # kwargs
    # vnc_disable       :   do not show vnc console
    # wait_vnc          :   wait until vnc finishes if vnc_enabled
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


class MemoryReadProcess(process_manager.ProcWorker):
    def __init__(self, input_path, machine_memory_size,
                 conn, machine, result_queue):
        self.input_path = input_path
        self.result_queue = result_queue
        self.machine_memory_size = machine_memory_size*1024
        self.total_read_size = 0
        self.conn = conn
        self.machine = machine

        self.manager = multiprocessing.Manager()
        self.memory_snapshot_size = self.manager.list()
        super(MemoryReadProcess, self).__init__(target=self.read_mem_snapshot)

    def read_mem_snapshot(self):
        # create memory snapshot aligned with 4KB
        time_s = time()
        is_first_recv = False
        time_first_recv = 0
        UPDATE_SIZE  = 1024*1024*10 # 10MB
        prev_processed_size = 0
        prev_processed_time = time()
        cur_processed_size = 0

        for repeat in xrange(100):
            if os.path.exists(self.input_path) == False:
                print "waiting for %s: " % self.input_path
                sleep(0.1)
        try:
            self.in_fd = open(self.input_path, 'rb')
            self.total_read_size = 0
            # read first 40KB and aligen header with 4KB
            data = self.in_fd.read(Memory.Memory.RAM_PAGE_SIZE*10)
            if is_first_recv == False:
                is_first_recv = True
                time_first_recv = time()

            libvirt_header = memory_util._QemuMemoryHeaderData(data)
            original_header = libvirt_header.get_header()
            align_size = Memory.Memory.RAM_PAGE_SIZE*2
            new_header = libvirt_header.get_aligned_header(align_size)
            self.result_queue.put(new_header)
            self.total_read_size += len(new_header)
            self.result_queue.put(data[len(original_header):])
            self.total_read_size += len(data[len(original_header):])
            LOG.info("Header size of memory snapshot is %s" % len(new_header))

            # write rest of the memory data
            prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)
            while True:
                input_fd = [self.control_queue._reader.fileno(), self.in_fd]
                input_ready, out_ready, err_ready = select.select(input_fd, [], [])
                if self.control_queue._reader.fileno() in input_ready:
                    control_msg = self.control_queue.get()
                    self._handle_control_msg(control_msg)
                if self.in_fd in input_ready:
                    data = self.in_fd.read(VMOverlayCreationMode.PIPE_ONE_ELEMENT_SIZE)
                    if data == None or len(data) <= 0:
                        break
                    current_size = len(data)
                    self.result_queue.put(data)
                    self.total_read_size += current_size
                    prog_bar.set_percent(100.0*self.total_read_size/self.machine_memory_size)
                    prog_bar.show_progress()

                    if self.total_read_size - prev_processed_size >= UPDATE_SIZE:
                        cur_time = time()
                        throughput = float((self.total_read_size-prev_processed_size)/(cur_time-prev_processed_time))
                        prev_processed_size = self.total_read_size
                        prev_processed_time = cur_time
                        self.monitor_current_bw = (throughput/Const.CHUNK_SIZE)

            prog_bar.finish()
        except Exception, e:
            sys.stdout.write("[MemorySnapshotting] Exception1n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.result_queue.put(Const.QUEUE_FAILED_MESSAGE)
        else:
            self.result_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        time_e = time()
        self.process_info['is_alive'] = False
        #LOG.debug("[time] Memory size of launch VM: %ld" % (self.total_read_size))
        LOG.debug("[time] Memory snapshotting first input at : %f" % (time_first_recv))
        LOG.debug("[time] memory snapshotting (%f ~ %f): %f, %f GBps" % (
            time_s, time_e, (time_e-time_s),
            (self.total_read_size/(time_e-time_s)/1024/1024/1024)))
        self.memory_snapshot_size.append(self.total_read_size)

    def get_memory_snapshot_size(self):
        if len(self.memory_snapshot_size) > 0:
            return long(self.memory_snapshot_size[0])
        return long(-1)

    def finish(self):
        if os.path.exists(self.input_path) == True:
            os.remove(self.input_path)
        if self.machine is not None:
            _terminate_vm(self.conn, self.machine)
            self.machine = None


class ProcessingProcess(multiprocessing.Process):
    def __init__(self, data_pipe, outpath):
        self.data_pipe = data_pipe
        self.outpath = outpath
        multiprocessing.Process.__init__(self, target=self.process)

    def process(self):
        recv_pipe, send_pipe = self.data_pipe
        send_pipe.close()
        self.out_fd = open(self.outpath, 'wb')
        total_bytes = 0
        while True:
            try:
                data = recv_pipe.recv()
                total_bytes += len(data)
                self.out_fd.write(data)
            except EOFError:
                break
        LOG.debug("finish receiving: %ld bytes" % total_bytes)
        self.out_fd.close()


class LibvirtThread(threading.Thread):
    def __init__(self, machine, outputpath):
        self.machine = machine
        self.outputpath = outputpath
        threading.Thread.__init__(self, target=self.save_mem)

    def save_mem(self):
        self.machine.save(self.outputpath)


def save_mem_snapshot(conn, machine, output_queue, **kwargs):
    #Set migration speed
    nova_util = kwargs.get('nova_util', None)
    fuse_stream_monitor = kwargs.get('fuse_stream_monitor', None)
    ret = machine.migrateSetMaxSpeed(1000000, 0)   # 1000 Gbps, unlimited
    if ret != 0:
        raise CloudletGenerationError("Cannot set migration speed : %s", machine.name())

    # Pause VM
    state, reason = machine.state(0)
    if state != libvirt.VIR_DOMAIN_PAUSED:
        machine.suspend()

    # Stop monitoring for memory access (snapshot will create a lot of access)
    fuse_stream_monitor.del_path(cloudletfs.StreamMonitor.MEMORY_ACCESS)
    if fuse_stream_monitor is not None:
        fuse_stream_monitor.terminate()
        fuse_stream_monitor.join()

    # get VM information
    machine_memory_size = machine.memoryStats().get('actual', None)
    if machine_memory_size is None:
        # libvirt <= 0.9.3
        xml = ElementTree.fromstring(machine.XMLDesc(libvirt.VIR_DOMAIN_XML_SECURE))
        memory_element = xml.find('memory')
        if memory_element is not None:
            machine_memory_size = long(memory_element.text)

    #Save memory state
    LOG.info("save VM memory state")
    try:
        fifo_path = NamedTemporaryFile(prefix="cloudlet-memory-snapshot-",
                                       delete=True)
        named_pipe_output = fifo_path.name + ".fifo"
        if os.path.exists(named_pipe_output):
            os.remove(named_pipe_output)
        os.mkfifo(named_pipe_output)
        memory_read_proc = MemoryReadProcess(named_pipe_output,
                                             machine_memory_size,
                                             conn,
                                             machine,
                                             output_queue)
        memory_read_proc.start()
        libvirt_thread = LibvirtThread(machine, named_pipe_output)
        libvirt_thread.start()
    except libvirt.libvirtError, e:
        # we intentionally ignore seek error from libvirt since we have cause
        # that by using named pipe
        if str(e).startswith('unable to seek') == False:
            raise CloudletGenerationError("libvirt memory save : " + str(e))
    finally:
        pass

    # TODO: update this to work with streaming
    try:
        if nova_util != None:
            nova_util.chown(fout_path, os.getuid())
    except memory_util.MachineGenerationError, e:
        raise CloudletGenerationError("Machine Generation Error: " + str(e))
    finally:
        pass

    if ret != 0:
        raise CloudletGenerationError("libvirt: Cannot save memory state")

    return memory_read_proc


def run_snapshot(conn, disk_image, mem_snapshot, new_xml_string, resume_time=None):
    if resume_time != None:
        start_resume_time = time()

    # resume
    restore_with_config(conn, mem_snapshot, new_xml_string)
    if resume_time != None:
        resume_time['start_time'] = start_resume_time
        resume_time['end_time'] = time()
        LOG.info("[RESUME] : QEMU resume time (%f)~(%f)=(%f)" % \
                (resume_time['start_time'], resume_time['end_time'], \
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
    vnc_process = subprocess.Popen(["gvncviewer", "%s:%d" % (vnc_ip, vnc_port)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=True)
    if no_wait == True:
        return

    LOG.info("waiting for finishing VNC interaction")
    try:
        vnc_process.wait()
    except KeyboardInterrupt as e:
        LOG.info("keyboard interrupt while waiting VNC")
        vnc_process.terminate()


def rettach_nic(machine, old_xml, new_xml, **kwargs):
    #kwargs
    #LOG = log object for nova
    #nova_util = nova_util is executioin wrapper for nova framework
    #           You should use nova_util in OpenStack, or subprocess
    #           will be returned without finishing their work
    # get xml info of running xml

    old_xml = ElementTree.fromstring(old_xml)
    old_nic = old_xml.find('devices/interface')
    filter_element = old_nic.find("filterref")
    if filter_element != None:
        old_nic.remove(filter_element)
    old_nic_xml = ElementTree.tostring(old_nic)
    ret = machine.detachDevice(old_nic_xml)
    if ret != 0:
        LOG.warning("failed to detach device")
    sleep(2)

    #attach
    new_xml = ElementTree.fromstring(new_xml)
    new_nic = new_xml.find('devices/interface')
    filter_element = new_nic.find("filterref")
    if filter_element != None:
        new_nic.remove(filter_element)
    new_nic_xml = ElementTree.tostring(new_nic)

    retry_count = 1
    while retry_count <= 3:
        try:
            ret = machine.attachDevice(new_nic_xml)
            if ret != 0:
                LOG.warning("failed to attach device (trying..%d)" % retry_count)
            LOG.info("success to rettach nic device")
            break
        except libvirt.libvirtError, e:
            LOG.info("Failed to rettach NIC")
            LOG.info(str(e))
            sleep(2)
        retry_count += 1



def restore_with_config(conn, mem_snapshot, xml):
    try:
        LOG.info("restoring VM...")
        conn.restoreFlags(mem_snapshot, xml, libvirt.VIR_DOMAIN_SAVE_RUNNING)
        LOG.info("VM is restored...")
    except libvirt.libvirtError, e:
        msg = "Error, make sure previous VM is closed and check QEMU_ARGUMENT"
        message = "%s\nXML: %s\n%s" % \
                (xml, str(e), msg)
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
    fout = open(out_path, 'wrb')
    hdr = memory_util._QemuMemoryneader(fin)

    # Write header
    hdr.xml = xml
    hdr.write(fout)
    fout.flush()

    # move to the content
    hdr.seek_body(fin)
    fout.write(fin.read())


def _waiting_to_finish(process_controller, worker_name):
    while True:
        process_info = process_controller.process_infos.get(worker_name, None)
        if process_info == None:
            raise CloudletGenerationError("Failed to access %s worker" % worker_name)
        if process_info['is_alive'] == False:
            break
        else:
            sleep(0.01)


def create_residue(base_disk, base_hashvalue,
                   basedisk_hashdict, basemem_hashdict,
                   resumed_vm, options, original_deltalist):
    '''Get residue
    return overlay_metafile, overlay_files
    '''
    time_start = time()
    process_controller = process_manager.get_instance()
    #overlay_mode = VMOverlayCreationMode.get_serial_single_process()
    #overlay_mode = VMOverlayCreationMode.get_pipelined_single_process()
    #overlay_mode = VMOverlayCreationMode.get_pipelined_single_process_finite_queue()
    overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process()
    #overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()

    process_controller.set_mode(overlay_mode)
    LOG.info("* Overlay creation configuration")
    LOG.info("  - %s" % str(options))
    LOG.info("  - %s" % str(overlay_mode))

    # 1. sanity check
    if (options == None) or (isinstance(options, Options) == False):
        raise CloudletGenerationError("Given option is invalid: %s" % str(options))
    (base_diskmeta, base_mem, base_memmeta) = \
            Const.get_basepath(base_disk, check_exist=True)
    qemu_logfile = resumed_vm.qemu_logfile

    # 2. suspend VM and get monitoring information
    memory_snapshot_queue = multiprocessing.Queue(overlay_mode.QUEUE_SIZE_MEMORY_SNAPSHOT)
    residue_deltalist_queue = multiprocessing.Queue(maxsize=overlay_mode.QUEUE_SIZE_OPTIMIZATION)
    compdata_queue = multiprocessing.Queue(maxsize=overlay_mode.QUEUE_SIZE_COMPRESSION)

    vm_monitor = VMMonitor(resumed_vm.conn, resumed_vm.machine, options,
                           resumed_vm.monitor,
                           base_disk, base_mem,
                           resumed_vm.resumed_disk,
                           qemu_logfile,
                           original_deltalist=original_deltalist)
    monitoring_info = vm_monitor.get_monitoring_info()

    time_ss = time()
    LOG.debug("[time] serialized step (%f ~ %f): %f" % (time_start,
                                                           time_ss,
                                                           (time_ss-time_start)))

    memory_read_proc = save_mem_snapshot(resumed_vm.conn,
                                         resumed_vm.machine,
                                         memory_snapshot_queue,
                                         fuse_stream_monitor=resumed_vm.monitor)

    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "MemoryReadProcess")

    # to be deleted
    #memory_read_proc.join()
    #while True:
    #    data = memory_snapshot_queue.get()
    #    if data == Const.QUEUE_SUCCESS_MESSAGE:
    #        break
    #import pdb;pdb.set_trace()

    # 3. get overlay VM (semantic gap + deduplication)
    dedup_proc = get_overlay_deltalist(monitoring_info, options,
                                       overlay_mode,
                                       base_disk, base_mem,
                                       base_memmeta,
                                       basedisk_hashdict,
                                       basemem_hashdict,
                                       resumed_vm.resumed_disk,
                                       memory_snapshot_queue,
                                       residue_deltalist_queue,
                                       process_controller)
    time_dedup = time()
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "DeltaDedup")

    # 4. compression
    LOG.info("Compressing overlay blobs")
    compress_proc = compression.CompressProc(residue_deltalist_queue,
                                             compdata_queue,
                                             overlay_mode)
    compress_proc.start()
    time_dedup = time()
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "CompressProc")


    time_packaging_start = time()
    # to be deleted
    #sleep(10000)
    if overlay_mode.OUTPUT_DESTINATION.startswith("network"):
        from stream_client import StreamSynthesisClient
        client = StreamSynthesisClient(base_hashvalue, compdata_queue)
        client.start()  # blocked
    elif overlay_mode.OUTPUT_DESTINATION.startswith("file"):
        overlay_info = list()
        overlay_files = list()
        overlay_metapath = os.path.join(os.getcwd(), Const.OVERLAY_META)
        comp_file_counter = 0
        temp_compfile_dir = mkdtemp(prefix="cloudlet-comp-")
        while True:
            comp_task = compdata_queue.get()
            if comp_task == Const.QUEUE_SUCCESS_MESSAGE:
                break
            if comp_task == Const.QUEUE_FAILED_MESSAGE:
                LOG.error("Failed to get compressed data")
                break
            (blob_comp_type, compdata, disk_chunks, memory_chunks) = comp_task
            blob_filename = os.path.join(temp_compfile_dir, "%s-stream-%d" %\
                                        (Const.OVERLAY_FILE_PREFIX,
                                        comp_file_counter))
            #LOG.debug("%s --> %d" % (blob_filename, blob_comp_type))
            #LOG.debug("%s: # of delta memory: %d\t# of delta disk: %d" %\
            #          (blob_filename, len(memory_chunks), len(disk_chunks)))
            comp_file_counter += 1
            overlay_files.append(blob_filename)
            output_fd = open(blob_filename, "wb+")
            output_fd.write(compdata)
            output_fd.close()
            blob_dict = {
                Const.META_OVERLAY_FILE_NAME:os.path.basename(blob_filename),
                Const.META_OVERLAY_FILE_COMPRESSION: blob_comp_type,
                Const.META_OVERLAY_FILE_SIZE:os.path.getsize(blob_filename),
                Const.META_OVERLAY_FILE_DISK_CHUNKS: disk_chunks,
                Const.META_OVERLAY_FILE_MEMORY_CHUNKS: memory_chunks
                }
            overlay_info.append(blob_dict)

        process_controller.terminate()
        cpu_stat = process_controller.cpu_statistics
        open("cpu-stat.json", "w+").write(json.dumps(cpu_stat))

        # wait until VM snapshotting finishes to get final VM memory snapshot size
        memory_read_proc.join()
        memory_read_proc.finish()   # deallocate resources for snapshotting
        memory_snapshot_size = memory_read_proc.get_memory_snapshot_size()
        LOG.debug("Memory Snapshot size: %ld" % memory_snapshot_size)
        overlay_metafile = _generate_overlaymeta(overlay_metapath,
                                                overlay_info,
                                                base_hashvalue,
                                                os.path.getsize(resumed_vm.resumed_disk),
                                                memory_snapshot_size)

        # 6. packaging VM overlay into a single zip file
        temp_dir = mkdtemp(prefix="cloudlet-overlay-")
        overlay_zipfile = os.path.join(temp_dir, Const.OVERLAY_ZIP)
        VMOverlayPackage.create(overlay_zipfile, overlay_metafile, overlay_files)
        time_packaging_end = time()
        LOG.debug("[time] Time for overlay packaging (%f ~ %f): %f" % (time_packaging_start,
                                                                    time_packaging_end,
                                                                    (time_packaging_end-time_packaging_start)))


        # 7. terminting
        resumed_vm.machine = None   # protecting malaccess to machine 
        if os.path.exists(overlay_metafile) == True:
            os.remove(overlay_metafile)
        if os.path.exists(temp_compfile_dir) == True:
            shutil.rmtree(temp_compfile_dir)
        time_end = time()

        LOG.debug("[time] Total residue creation time (%f ~ %f): %f" % (time_start, time_end,
                                                                (time_end-time_start)))
        return overlay_zipfile


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
            index = DeltaItem.get_index(DeltaItem.DELTA_MEMORY, mem_chunk*Memory.Memory.RAM_PAGE_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            overlay_mem_chunks[mem_chunk] = {"blob_name":blob_name, 'chunk_size':chunk_size}
        for disk_chunk in disk_chunks:
            index = DeltaItem.get_index(DeltaItem.DELTA_DISK, disk_chunk*Const.CHUNK_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            overlay_disk_chunks[disk_chunk] = {"blob_name":blob_name, 'chunk_size':chunk_size}
        # (memory, memory_total, disk, disk_total)
        access_per_blobs[blob_name] = {
                'mem_access':0, 'mem_access_size':0, 'mem_total':len(memory_chunks),
                'disk_access':0, 'disk_access_size':0, 'disk_total':len(disk_chunks),
                'blob_size':each_file[Const.META_OVERLAY_FILE_SIZE]}
        total_overlay_mem_chunks += len(memory_chunks)
        total_overlay_disk_chunks += len(disk_chunks)

    # compare real accessed chunks with overlay chunk list
    overlay_mem_access_count = 0
    overlay_disk_access_count = 0
    overlay_mem_access_size = 0
    overlay_disk_access_size = 0
    for access_chunk in mem_access_list:
        if overlay_mem_chunks.get(access_chunk, None) != None:
            index = DeltaItem.get_index(DeltaItem.DELTA_MEMORY, access_chunk*Memory.Memory.RAM_PAGE_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            blob_name = overlay_mem_chunks.get(access_chunk)['blob_name']
            chunk_size = overlay_mem_chunks.get(access_chunk)['chunk_size']
            access_per_blobs[blob_name]['mem_access'] += 1 # 0: memory
            access_per_blobs[blob_name]['mem_access_size'] += chunk_size
            overlay_mem_access_count += 1
            overlay_mem_access_size += chunk_size
    for access_chunk in disk_access_list:
        if overlay_disk_chunks.get(access_chunk, None) != None:
            index = DeltaItem.get_index(DeltaItem.DELTA_DISK, access_chunk*Const.CHUNK_SIZE)
            chunk_size = len(delta_dic[index].get_serialized())
            blob_name = overlay_disk_chunks.get(access_chunk)['blob_name']
            chunk_size = overlay_disk_chunks.get(access_chunk)['chunk_size']
            access_per_blobs[blob_name]['disk_access'] += 1
            access_per_blobs[blob_name]['disk_access_size'] += chunk_size
            overlay_disk_access_count += 1
            overlay_disk_access_size += chunk_size

    LOG.debug("-------------------------------------------------")
    LOG.debug("## Synthesis Statistics (took %f seconds) ##" % (time()-start_time))
    LOG.debug("Overlay acccess count / total overlay count\t: %d / %d = %05.2f %%" % \
            (overlay_mem_access_count+overlay_disk_access_count,\
            total_overlay_mem_chunks+total_overlay_disk_chunks, \
            100.0 * (overlay_mem_access_count+overlay_disk_access_count)/ (total_overlay_mem_chunks+total_overlay_disk_chunks)))
    LOG.debug("Overlay acccess size / total overlay size\t: %10.3d MB/ %10.3f MB= %05.2f %%" % \
            ((overlay_mem_access_size+overlay_disk_access_size)/1024.0/1024, \
            (total_overlay_size/1024.0/1024),\
            100.0 * (overlay_mem_access_size+overlay_disk_access_size)/total_overlay_size))
    try:
        LOG.debug("  Memory Count: Overlay memory acccess / total memory overlay\t: %d / %d = %05.2f %%" % \
                (overlay_mem_access_count, total_overlay_mem_chunks,\
                100.0 * overlay_mem_access_count/total_overlay_mem_chunks))
        LOG.debug("  Memory Size: Overlay memory acccess / total overlay\t: %d / %d = %05.2f %%" % \
                (overlay_mem_access_size, total_overlay_size,\
                100.0 * overlay_mem_access_size/total_overlay_size))
        LOG.debug("  Disk Count: Overlay acccess / total disk overlay\t: %d / %d = %05.2f %%" % \
                (overlay_disk_access_count, total_overlay_disk_chunks, \
                100.0 * overlay_disk_access_count/total_overlay_disk_chunks))
        LOG.debug("  Disk Size: Overlay acccess / total overlay\t: %d / %d = %05.2f %%" % \
                (overlay_disk_access_size, total_overlay_size, \
                100.0 * overlay_disk_access_size/total_overlay_size))
        LOG.debug("  EXTRA (count): Overlay memory acccess / VM memory access\t: %d / %d = %05.2f %%" % \
                (overlay_mem_access_count, len(mem_access_list), \
                100.0 * overlay_mem_access_count/len(mem_access_list)))
        LOG.debug("  EXTRA (count): Overlay disk acccess / VM disk access\t: %d / %d = %05.2f %%" % \
                (overlay_disk_access_count, len(disk_access_list), \
                100.0 * overlay_disk_access_count/len(disk_access_list)))
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
    LOG.debug("%d blobs (%f MB) are required out of %d (%05.2f %%)" % \
            (used_blob_count, used_blob_size/1024.0/1024, len(access_per_blobs.keys()), \
            used_blob_count*100.0/len(access_per_blobs.keys())))
    LOG.debug("-------------------------------------------------")



'''External API Start
'''
def validate_congifuration():
    if os.path.exists(Const.QEMU_BIN_PATH) == False:
        LOG.error("KVM/QEMU does not exist at %s" % Const.QEMU_BIN_PATH)
        return False

    cmd = ["%s" % Const.QEMU_BIN_PATH, "--version"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
            close_fds=True)
    out, err = proc.communicate()
    if len(err) > 0:
        LOG.error("KVM validation Error: %s" % (err))
        return False
    if out.find("Cloudlet") < 0:
        LOG.error("KVM validation Error, Incorrect Version:\n%s" % (out))
        return False
    return True


# share with OpenStack
def _create_baseVM(conn, domain, base_diskpath, base_mempath, base_diskmeta, base_memmeta, **kwargs):
    """ generate base vm given base_diskpath
    Args:
        base_diskpath: path to disk image exists
        base_mempath : target path to generate base mem
        base_diskmeta : target path to generate basedisk hashlist
        base_memmeta : target path to generate basemem hashlist
    """
    # make memory snapshot
    # VM has to be paused first to perform stable disk hashing
    save_mem_snapshot(conn, domain, base_mempath, **kwargs)
    LOG.info("Start Base VM Memory hashing")
    base_mem = Memory.hashing(base_mempath)
    base_mem.export_to_file(base_memmeta)
    LOG.info("Finish Base VM Memory hashing")

    # generate disk hashing
    # TODO: need more efficient implementation, e.g. bisect
    LOG.info("Start Base VM Disk hashing")
    base_hashvalue = Disk.hashing(base_diskpath, base_diskmeta)
    LOG.info("Finish Base VM Disk hashing")
    return base_hashvalue


def create_baseVM(disk_image_path):
    # Create Base VM(disk, memory) snapshot using given VM disk image
    # :param disk_image_path : file path of the VM disk image
    # :returns: (generated base VM disk path, generated base VM memory path)

    # Check DB
    disk_image_path = os.path.abspath(disk_image_path)
    (base_diskmeta, base_mempath, base_memmeta) = \
            Const.get_basepath(disk_image_path)

    # check sanity
    if not os.path.exists(Const.TEMPLATE_XML):
        raise CloudletGenerationError("Cannot find Base VM default XML at %s\n" \
                % Const.TEMPLATE_XML)
    if not os.path.exists(Const.TEMPLATE_OVF):
        raise CloudletGenerationError("Cannot find ovf file for AMIt %s\n" \
                % Const.TEMPLATE_OVF)
    if os.path.exists(base_mempath):
        warning_msg = "Warning: (%s) exist.\nAre you sure to overwrite? (y/N) " \
                % (base_mempath)
        ret = raw_input(warning_msg)
        if str(ret).lower() != 'y':
            sys.exit(1)

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
    xml = ElementTree.fromstring(open(Const.TEMPLATE_XML, "r").read())
    xml, new_xml_string = _convert_xml(disk_path=disk_image_path, xml=xml)

    # launch VM & wait for end of vnc
    machine = None
    try:
        machine = run_vm(conn, new_xml_string, wait_vnc=True)
        base_hashvalue = _create_baseVM(conn, machine, disk_image_path, base_mempath, 
                base_diskmeta, base_memmeta)
    except Exception as e:
        LOG.error("failed at %s" % str(traceback.format_exc()))
        if machine != None:
            _terminate_vm(conn, machine)
            machine = None
        sys.exit(1)

    # save the result to DB
    dbconn = db_api.DBConnector()
    basevm_list = dbconn.list_item(db_table.BaseVM)
    for item in basevm_list:
        if disk_image_path == item.disk_path: 
            dbconn.del_item(item)
            break
    new_basevm = db_table.BaseVM(disk_image_path, base_hashvalue)
    dbconn.add_item(new_basevm)

    # write hashvalue to file
    hashfile_path = Const.get_base_hashpath(disk_image_path)
    open(hashfile_path, "w+").write(str(base_hashvalue) + "\n")

    # write protection
    #os.chmod(disk_image_path, stat.S_IRUSR)
    #os.chmod(base_diskmeta, stat.S_IRUSR)
    #os.chmod(base_mempath, stat.S_IRUSR)
    #os.chmod(base_memmeta, stat.S_IRUSR)
    #os.chmod(hashfile_path, stat.S_IRUSR)

    return disk_image_path, base_mempath


class PreloadResidueData(threading.Thread):
    def __init__(self, base_diskmeta, base_memmeta):
        self.base_diskmeta = base_diskmeta
        self.base_memmeta = base_memmeta
        self.basedisk_hashdict = None
        self.basmem_hashdict = None
        threading.Thread.__init__(self, target=self.preloading)

    def preloading(self):
        self.basedisk_hashdict = delta.DeltaDedup.disk_import_hashdict(self.base_diskmeta)
        self.basemem_hashdict = delta.DeltaDedup.memory_import_hashdict(self.base_memmeta)


def _reconstruct_mem_deltalist(base_disk, base_mem, overlay_filepath):
    ret_deltalist = list()
    deltalist = DeltaList.fromfile(overlay_filepath)

    #const
    import struct
    import tool
    import mmap

    # initialize reference data to use mmap
    base_disk_fd = open(base_disk, "rb")
    raw_disk = mmap.mmap(base_disk_fd.fileno(), 0, prot=mmap.PROT_READ)
    base_mem_fd = open(base_mem, "rb")
    raw_mem = mmap.mmap(base_mem_fd.fileno(), 0, prot=mmap.PROT_READ)
    ZERO_DATA = struct.pack("!s", chr(0x00)) * Const.CHUNK_SIZE
    chunk_size = Const.CHUNK_SIZE
    recovered_data_dict = dict()

    for delta_item in deltalist:
        if type(delta_item) != DeltaItem:
            raise CloudletGenerationError("Failed to reconstruct deltalist")

        #LOG.info("recovering %ld/%ld" % (index, len(delta_list)))
        if (delta_item.ref_id == DeltaItem.REF_RAW):
            recover_data = delta_item.data
        elif (delta_item.ref_id == DeltaItem.REF_ZEROS):
            recover_data = ZERO_DATA
        elif (delta_item.ref_id == DeltaItem.REF_BASE_MEM):
            offset = delta_item.data
            recover_data = raw_mem[offset:offset+chunk_size]
        elif (delta_item.ref_id == DeltaItem.REF_BASE_DISK):
            offset = delta_item.data
            recover_data = raw_disk[offset:offset+chunk_size]
        elif delta_item.ref_id == DeltaItem.REF_SELF:
            ref_index = delta_item.data
            self_ref_data = recovered_data_dict.get(ref_index, None)
            if self_ref_data == None:
                msg = "Cannot find self reference: type(%ld), offset(%ld), \
                        index(%ld), ref_index(%ld)" % \
                        (delta_item.delta_type, delta_item.offset, \
                        delta_item.index, ref_index)
                raise MemoryError(msg)
            recover_data = self_ref_data
        elif delta_item.ref_id == DeltaItem.REF_XDELTA:
            patch_data = delta_item.data
            patch_original_size = delta_item.offset_len
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY:
                base_data = raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
            elif delta_item.delta_type == DeltaItem.DELTA_DISK:
                base_data = raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
            else:
                msg = "Delta should be either disk or memory"
                raise CloudletGenerationError(msg)
            recover_data = tool.merge_data(base_data, patch_data, len(base_data)*5)
        else:
            msg ="Cannot recover: invalid referce id %d" % delta_item.ref_id
            raise MemoryError(msg)

        if len(recover_data) != delta_item.offset_len:
            msg = "Recovered Size Error: %d, ref_id: %s, %ld %ld" % \
                    (len(recover_data), delta_item.ref_id, \
                    delta_item.data_len, delta_item.offset)
            raise CloudletGenerationError(msg)

        # recover
        #delta_item.ref_id = DeltaItem.REF_RAW
        #delta_item.data = recover_data
        #delta_item.data_len = len(recover_data)
        delta_item.hash_value = hashlib.sha256(recover_data).digest()
        recovered_data_dict[delta_item.index] = recover_data
        ret_deltalist.append(delta_item)

    base_disk_fd.close()
    base_mem_fd.close()
    raw_disk.close()
    raw_disk = None
    raw_mem.close()
    raw_mem = None
    recovered_data_dict = None

    return ret_deltalist


def synthesis(base_disk, overlay_path, **kwargs):
    # VM Synthesis and run recoverd VM
    # param base_disk : path to base disk
    # param overlay_path: path to VM overlay file
    # param disk_only: synthesis size VM with only disk image
    # param return_residue: return residue of changed portion
    if os.path.exists(base_disk) == False:
        msg = "Base disk does not exist at %s" % base_disk
        raise CloudletGenerationError(msg)

    disk_only = kwargs.get('disk_only', False)
    zip_container = kwargs.get('zip_container', False)
    return_residue = kwargs.get('return_residue', False)
    qemu_args = kwargs.get('qemu_args', False)

    nova_xml = kwargs.get('nova_xml', None)
    base_mem = kwargs.get('base_mem', None)
    base_diskmeta = kwargs.get('base_diskmeta', None)
    base_memmeta = kwargs.get('base_memmeta', None)

    overlay_filename = NamedTemporaryFile(prefix="cloudlet-overlay-file-")
    decompe_time_s = time()
    if zip_container == False:
        if os.path.exists(overlay_path) == False:
            msg = "VM overlay does not exist at %s" % overlay_path
            raise CloudletGenerationError(msg)
        LOG.info("Decompressing VM overlay")
        meta_info = compression.decomp_overlay(overlay_path, overlay_filename.name)
    else:
        meta_info = compression.decomp_overlayzip(overlay_path, overlay_filename.name)

    LOG.info("Decompression time : %f (s)" % (time()-decompe_time_s))
    LOG.info("Recovering launch VM")
    launch_disk, launch_mem, fuse, delta_proc, fuse_thread = \
            recover_launchVM(base_disk, meta_info, overlay_filename.name, \
            **kwargs)
    # resume VM
    LOG.info("Resume the launch VM")
    synthesized_VM = SynthesizedVM(launch_disk, launch_mem, fuse,
            disk_only=disk_only, qemu_args=qemu_args, nova_xml=nova_xml)

    # no-pipelining
    delta_proc.start()
    fuse_thread.start()
    delta_proc.join()
    fuse_thread.join()

    synthesized_VM.resume()
    if return_residue == True:
        # preload basevm hash dictionary for creating residue
        (base_diskmeta, base_mem, base_memmeta) = \
                Const.get_basepath(base_disk, check_exist=True)
        preload_thread = PreloadResidueData(base_diskmeta, base_memmeta)
        preload_thread.daemon = True
        preload_thread.start()
    connect_vnc(synthesized_VM.machine)

    # statistics
    synthesized_VM.monitor.terminate()
    synthesized_VM.monitor.join()
    #mem_access_list = synthesized_VM.monitor.mem_access_chunk_list
    #disk_access_list = synthesized_VM.monitor.disk_access_chunk_list
    #synthesis_statistics(meta_info, overlay_filename.name, \
    #        mem_access_list, disk_access_list)

    if return_residue == True:
        options = Options()
        options.TRIM_SUPPORT = True
        options.FREE_SUPPORT = True
        options.DISK_ONLY = False
        try:
            # FIX: currently we revisit all overlay to reconstruct hash information
            # we can leverage Recovered_delta class reconstruction process,
            # but that does not generate hash value
            (base_diskmeta, base_mem, base_memmeta) = \
                    Const.get_basepath(base_disk, check_exist=True)
            time_a = time()
            prev_mem_deltalist = _reconstruct_mem_deltalist( \
                    base_disk, base_mem, overlay_filename.name)
            time_b = time()
            LOG.debug("[time] Time for reconstructing previous deltalist (%f ~ %f): %f" %
                      (time_b, time_a, (time_b-time_a)))
            preload_thread.join()
            time_c = time()
            LOG.debug("[time] Time for waiting to build hashdict: %f" %
                      (time_c-time_b))
            residue_overlay = create_residue(base_disk,
                                             meta_info[Const.META_BASE_VM_SHA256],
                                             preload_thread.basedisk_hashdict,
                                             preload_thread.basemem_hashdict,
                                             synthesized_VM,
                                             options,
                                             prev_mem_deltalist)
            LOG.info("[RESULT] Residue")
            LOG.info("[RESULT]   Metafile : %s" % \
                    (os.path.abspath(residue_overlay)))
        except CloudletGenerationError, e:
            LOG.error("Cannot create residue : %s" % (str(e)))

    # terminate
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
        modified_disk_chunk_count += len(item[Const.META_OVERLAY_FILE_DISK_CHUNKS])
        modified_memory_chunk_count += len(item[Const.META_OVERLAY_FILE_MEMORY_CHUNKS])
    output = "VM overlay\t\t\t: %s\n" % overlay_path
    output += "Base VM ID\t\t\t: %s\n" % baseVMsha256
    output += "# of modified disk chunk\t: %s\n" % modified_disk_chunk_count
    output += "# of modified memory chunk\t: %s\n" % modified_memory_chunk_count
    output += "VM disk size\t\t\t: %s bytes\n" % vm_disk_size
    output += "VM memory size\t\t\t: %s bytes\n" % vm_memory_size
    return  output


'''External API End
'''


def main(argv):
    MODE = ('base', 'overlay', 'synthesis', "test")
    USAGE = 'Usage: %prog ' + ("[%s]" % "|".join(MODE)) + " [paths..]"
    VERSION = 'Cloudlet VM synthesis: %s' % Const.VERSION
    DESCRIPTION = 'Cloudlet Overlay Generation & Synthesis'
    if not validate_congifuration():
        sys.stderr.write("failed to validate configuration\n")
        sys.exit(1)

    parser = OptionParser(usage=USAGE, version=VERSION, description=DESCRIPTION)
    opts, args = parser.parse_args()
    if len(args) == 0:
        parser.error("Incorrect mode among %s" % "|".join(MODE))
    mode = str(args[0]).lower()

    if mode == MODE[0]: #base VM generation
        if len(args) != 2:
            parser.error("Generating base VM requires 1 arguements\n1) VM disk path")
            sys.exit(1)
        # creat base VM
        disk_image_path = args[1]
        disk_path, mem_path = create_baseVM(disk_image_path)
        LOG.info("\nBase VM is created from %s" % disk_image_path)
        LOG.info("Disk: %s" % disk_path)
        LOG.info("Mem: %s" % mem_path)
    elif mode == MODE[2]:   #synthesis
        if len(args) < 3:
            parser.error("Synthesis requires 2 arguments\n \
                    1) base-disk path\n \
                    2) overlay meta path\n \
                    3) disk if disk only")
            sys.exit(1)
        base_disk_path = args[1]
        meta = args[2]
        if len(args) == 4 and args[3] == 'disk':
            disk_only = True
        else:
            disk_only = False
        synthesis(base_disk_path, meta, disk_only=disk_only, return_residue=False)
    elif mode == 'seperate_overlay':
        if len(args) != 3:
            parser.error("seperating disk and memory overlay need 3 arguments\n \
                    1)meta file\n \
                    2)output directory\n")
            sys.exit(1)
        meta = args[1]
        output_dir = args[2]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        overlay_path = NamedTemporaryFile(prefix="cloudlet-qemu-log-")
        meta_info = compression.decomp_overlay(meta, overlay_path.name)

        comp_overlay_files = meta_info[Const.META_OVERLAY_FILES]
        comp_overlay_files = [item[Const.META_OVERLAY_FILE_NAME] for item in comp_overlay_files]
        comp_overlay_files = [os.path.join(os.path.dirname(meta), item) for item in comp_overlay_files]
        import shutil
        for comp_file in comp_overlay_files:
            filename = os.path.join(output_dir, os.path.basename(comp_file))
            shutil.copyfile(comp_file, filename)
        delta_list = DeltaList.fromfile(overlay_path.name)
        memory_delta_list = list()
        disk_delta_list = list()
        for delta_item in delta_list:
            if delta_item.delta_type == DeltaItem.DELTA_MEMORY:
                memory_delta_list.append(delta_item)
            elif delta_item.delta_type == DeltaItem.DELTA_DISK:
                disk_delta_list.append(delta_item)
            else:
                raise CloudletGenerationError("No delta type exist")

        disk_uncomp_data = ''
        memory_uncomp_data = ''
        for disk_delta_item in disk_delta_list:
            disk_uncomp_data += (disk_delta_item.get_serialized())
        for mem_delta_item in memory_delta_list:
            memory_uncomp_data += (mem_delta_item.get_serialized())

        total_size = len(disk_delta_list)+len(memory_delta_list)

        LOG.info("Delta Item #\tDisk: %d / %d = %f, Memory: %d / %d = %f" % \
                (len(disk_delta_list), total_size, 100.0*len(disk_delta_list)/total_size, \
                len(memory_delta_list), total_size, 100.0*len(memory_delta_list)/total_size))

        disk_uncomp_size = len(disk_uncomp_data)
        memory_uncomp_size = len(memory_uncomp_data)
        total_size = disk_uncomp_size+memory_uncomp_size
        LOG.info("Uncomp Size\tDisk: %d / %d = %f, Memory: %d / %d = %f" % \
                (disk_uncomp_size, total_size, 100.0*disk_uncomp_size/total_size, \
                memory_uncomp_size, total_size, 100.0*memory_uncomp_size/total_size))

        from lzma import LZMACompressor
        disk_comp_option = {'format':'xz', 'level':9}
        mem_comp_option = {'format':'xz', 'level':9}
        disk_comp = LZMACompressor(options=disk_comp_option)
        mem_comp = LZMACompressor(options=mem_comp_option)
        disk_comp_data = disk_comp.compress(disk_uncomp_data)
        disk_comp_data += disk_comp.flush()
        mem_comp_data = mem_comp.compress(memory_uncomp_data)
        mem_comp_data += mem_comp.flush()

        disk_comp_size = len(disk_comp_data)
        memory_comp_size = len(mem_comp_data)
        total_size = disk_comp_size+memory_comp_size
        LOG.info("Comp Size\tDisk: %d / %d = %f, Memory: %d / %d = %f" % \
                (disk_comp_size, total_size, 100.0*disk_comp_size/total_size, \
                memory_comp_size, total_size, 100.0*memory_comp_size/total_size))
        '''
        disk_overlay_path = os.path.join(output_dir, "disk_overlay")
        memory_overlay_path = os.path.join(output_dir, "memory_overlay")
        disk_blob_list = delta.divide_blobs(disk_delta_list, disk_overlay_path,
                Const.OVERLAY_BLOB_SIZE_KB, Const.CHUNK_SIZE,
                Memory.Memory.RAM_PAGE_SIZE)
        memory_blob_list = delta.divide_blobs(memory_delta_list, memory_overlay_path,
                Const.OVERLAY_BLOB_SIZE_KB, Const.CHUNK_SIZE,
                Memory.Memory.RAM_PAGE_SIZE)
        '''
    elif mode == "first_run":   #overlay VM creation
        import socket
        import struct

        start_time = time()
        if len(args) != 2:
            parser.error("Resume VM and wait for first run\n \
                    1) Base disk path\n")
            sys.exit(1)
        # create overlay
        disk_path = args[1]

        # waiting for socket command
        port = 10111
        serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversock.bind(("0.0.0.0", port))
        serversock.listen(1)
        serversock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        LOG.info("Waiting for client connection at %d.." % (port))
        (client_socket, address) = serversock.accept()
        app_name_size = struct.unpack("!I", client_socket.recv(4))[0]
        app_name = struct.unpack("!%ds" % app_name_size, client_socket.recv(app_name_size))[0]

        LOG.info("start VM resume for application: %s" % app_name)
        # start application & VM
        options = Options()
        options.DISK_ONLY = False
        vm_overlay = VM_Overlay(disk_path, options)
        machine = vm_overlay.resume_basevm()
        connect_vnc(machine)
        vm_overlay.create_overlay()

        LOG.info("overlay metafile : %s" % vm_overlay.overlay_metafile)
        LOG.info("overlay : %s" % str(vm_overlay.overlay_files[0]))
        LOG.info("overlay creation time: %f" % (time()-start_time()))

    elif mode == 'compress':
        if len(args) != 3:
            parser.error("recompress requires 2 arguments\n \
                    1)meta file\n \
                    2)output directory\n")
            sys.exit(1)
        meta = args[1]
        output_dir = args[2]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        overlay_path = os.path.join(output_dir, "overlay")
        meta_info = compression.decomp_overlay(meta, overlay_path)

        blob_size_list = [32, 64, 1024, 1024*1024]
        for order_type in ("access", "linear"):
            delta_list = DeltaList.fromfile(overlay_path)
            sub_dir1 = os.path.join(output_dir, order_type)
            if order_type == "access":
                pass
            elif order_type == "linear":
                delta.reorder_deltalist_linear(Memory.Memory.RAM_PAGE_SIZE, delta_list)
            elif order_type == "random":
                delta.reorder_deltalist_random(Memory.Memory.RAM_PAGE_SIZE, delta_list)

            for blob_size in blob_size_list:
                sub_dir = os.path.join(sub_dir1, "%d" % (blob_size))
                if not os.path.exists(sub_dir):
                    os.makedirs(sub_dir)
                meta_path = os.path.join(sub_dir, "overlay-meta")
                overlay_prefix = os.path.join(sub_dir, "overlay-blob")
                LOG.info("Creating %d KB overlays" % blob_size)
                blob_list = delta.divide_blobs(delta_list, overlay_prefix,
                        blob_size, Const.CHUNK_SIZE,
                        Memory.Memory.RAM_PAGE_SIZE)
                _update_overlay_meta(meta_info, meta_path, blob_info=blob_list)
                DeltaList.statistics(delta_list)
    elif mode == 'reorder':
        if len(args) != 5:
            parser.error("Reordering requires 4 arguments\n \
                    1)[linear | access-pattern file path]\n \
                    2)meta file\n \
                    3)blob size in kb\n \
                    4)output directory\n")
            sys.exit(1)

        access_pattern_file = args[1]
        meta = args[2]
        blob_size_kb = int(args[3])
        output_dir = args[4]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # decomp
        overlay_path = os.path.join(output_dir, "precise.overlay")
        new_meta_path = os.path.join(output_dir, "precise.overlay-meta")
        meta_info = compression.decomp_overlay(meta, overlay_path)
        delta_list = DeltaList.fromfile(overlay_path)
        # reorder
        if access_pattern_file == "linear":
            delta.reorder_deltalist_linear(Memory.Memory.RAM_PAGE_SIZE, delta_list)
        else:
            delta.reorder_deltalist_file(access_pattern_file,
                    Memory.Memory.RAM_PAGE_SIZE, delta_list)
        DeltaList.statistics(delta_list)
        blob_list = delta.divide_blobs(delta_list, overlay_path,
                blob_size_kb, Const.CHUNK_SIZE,
                Memory.Memory.RAM_PAGE_SIZE)
        _update_overlay_meta(meta_info, new_meta_path, blob_info=blob_list)
    else:
        LOG.warning("Invalid command")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    status = main(sys.argv)
    sys.exit(status)
