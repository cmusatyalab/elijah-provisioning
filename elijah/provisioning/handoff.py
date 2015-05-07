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

import time
import os
import sys
import select
import multiprocessing
import threading
import hashlib
import libvirt
import shutil
import traceback
import struct
import msgpack
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from xml.etree import ElementTree

import Memory
import Disk
import cloudletfs
import memory_util
from Configuration import Const
from Configuration import VMOverlayCreationMode
from Configuration import Options
from progressbar import AnimatedProgressBar
from package import VMOverlayPackage
import delta
from delta import DeltaList
from delta import DeltaItem
from tool import comp_lzma
from tool import diff_files
from progressbar import AnimatedProgressBar
from package import VMOverlayPackage
import compression
import process_manager
import qmp_af_unix
import log as logging

LOG = logging.getLogger(__name__)

# This is only for experiemental purpose.
# It is used to measure the time for changing network BW or CPU cores at the
# right time.
_handoff_start_time = [sys.maxint]

class HandoffError(Exception):
    pass

class PreloadResidueData(threading.Thread):
    def __init__(self, base_diskpath, overlay_filename):
        (self.base_diskmeta, self.base_mem, self.base_memmeta) = \
                Const.get_basepath(base_diskpath, check_exist=True)
        self.base_diskpath = base_diskpath
        self.overlay_filename = overlay_filename
        self.basedisk_hashdict = None
        self.basmem_hashdict = None
        threading.Thread.__init__(self, target=self.preloading)

    def preloading(self):
        self.basedisk_hashdict = delta.DeltaDedup.disk_import_hashdict(self.base_diskmeta)
        self.basemem_hashdict = delta.DeltaDedup.memory_import_hashdict(self.base_memmeta)
        # FIX: currently we revisit all overlay to reconstruct hash information
        # we can leverage Recovered_delta class reconstruction process,
        # but that does not generate hash value
        self.prev_mem_deltalist = self._reconstruct_mem_deltalist(self.base_diskpath,
                                                             self.base_mem,
                                                             self.overlay_filename)

    def _reconstruct_mem_deltalist(self, base_disk, base_mem, overlay_filepath):
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
        recovered_hash_dict = dict()

        for delta_item in deltalist:
            if type(delta_item) != DeltaItem:
                raise HandoffError("Failed to reconstruct deltalist")

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
            elif delta_item.ref_id == DeltaItem.REF_SELF_HASH:
                ref_hashvalue = delta_item.data
                self_ref_data = recovered_hash_dict.get(ref_hashvalue, None)
                if self_ref_delta_item == None:
                    return None
                recover_data = self_ref_data
                delta_item.hash_value = ref_hashvalue
            elif delta_item.ref_id == DeltaItem.REF_XDELTA:
                patch_data = delta_item.data
                patch_original_size = delta_item.offset_len
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                    base_data = raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                    base_data = raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
                else:
                    msg = "Delta should be either disk or memory"
                    raise HandoffError(msg)
                recover_data = tool.merge_data(base_data, patch_data, len(base_data)*5)
            elif delta_item.ref_id == DeltaItem.REF_BSDIFF:
                patch_data = delta_item.data
                patch_original_size = delta_item.offset_len
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                    base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                    base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
                else:
                    raise DeltaError("Delta type should be either disk or memory")
                recover_data = tool.merge_data_bsdiff(base_data, patch_data)
            elif delta_item.ref_id == DeltaItem.REF_XOR:
                patch_data = delta_item.data
                patch_original_size = delta_item.offset_len
                if delta_item.delta_type == DeltaItem.DELTA_MEMORY or\
                        delta_item.delta_type == DeltaItem.DELTA_MEMORY_LIVE:
                    base_data = self.raw_mem[delta_item.offset:delta_item.offset+patch_original_size]
                elif delta_item.delta_type == DeltaItem.DELTA_DISK or\
                    delta_item.delta_type == DeltaItem.DELTA_DISK_LIVE:
                    base_data = self.raw_disk[delta_item.offset:delta_item.offset+patch_original_size]
                else:
                    raise DeltaError("Delta type should be either disk or memory")
                recover_data = tool.cython_xor(base_data, patch_data)
            else:
                msg ="Cannot recover: invalid referce id %d" % delta_item.ref_id
                raise MemoryError(msg)

            if len(recover_data) != delta_item.offset_len:
                msg = "Recovered Size Error: %d, ref_id: %s, %ld %ld" % \
                        (len(recover_data), delta_item.ref_id, \
                        delta_item.data_len, delta_item.offset)
                raise HandoffError(msg)

            # recover
            #delta_item.ref_id = DeltaItem.REF_RAW
            #delta_item.data = recover_data
            #delta_item.data_len = len(recover_data)
            if delta_item.hash_value == None or len(delta_item.hash_value) == 0:
                delta_item.hash_value = hashlib.sha256(recover_data).digest()
            recovered_data_dict[delta_item.index] = recover_data
            recovered_hash_dict[delta_item.hash_value] = recover_data
            ret_deltalist.append(delta_item)

        base_disk_fd.close()
        base_mem_fd.close()
        raw_disk.close()
        raw_disk = None
        raw_mem.close()
        raw_mem = None
        recovered_data_dict = None
        recovered_hash_dict = None

        return ret_deltalist



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
        time1 = time.time()
        # do not pause VM since we're doing semi-live migration
        # this will be done by qmp
        #if self.machine is not None:
        #    vm_state, reason = self.machine.state(0)
        #    if vm_state != libvirt.VIR_DOMAIN_PAUSED:
        #        self.machine.suspend()

        time2 = time.time()
        # 2. get dma & discard information
        if self.options.TRIM_SUPPORT:
            dma_dict, trim_dict = Disk.parse_qemu_log(self.qemu_logfile, Const.CHUNK_SIZE)
            if len(trim_dict) == 0:
                LOG.warning("No TRIM Discard, Check /etc/fstab configuration")
        else:
            trim_dict = dict()
        free_memory_dict = dict()
        time3 = time.time()
        # 3. get used sector information from x-ray
        used_blocks_dict = None
        if self.options.XRAY_SUPPORT:
            import xray
            used_blocks_dict = xray.get_used_blocks(modified_disk)

        info_dict = dict()
        info_dict[_MonitoringInfo.DISK_USED_BLOCKS] = used_blocks_dict
        info_dict[_MonitoringInfo.DISK_FREE_BLOCKS] = trim_dict
        info_dict[_MonitoringInfo.MEMORY_FREE_BLOCKS] = free_memory_dict

        modified_chunk_queue = self.fuse_stream_monitor.get_modified_chunk_queue()
        time4 = time.time()
        # mark the modifid disk area in the original VM overlay as modified area
        if self.original_deltalist is not None:
            for o_delta_item in self.original_deltalist:
                if o_delta_item.delta_type == DeltaItem.DELTA_DISK:
                    modified_index = o_delta_item.offset / Const.CHUNK_SIZE
                    modified_chunk_queue.put((modified_index, 1.0))
        info_dict[_MonitoringInfo.DISK_MODIFIED_BLOCKS] = modified_chunk_queue
        time5 = time.time()

        LOG.debug("consumed time %f, %f, %f, %f" % ((time5-time4), (time4-time3), (time3-time2), (time2-time1)))

        self.monitoring_info = _MonitoringInfo(info_dict)
        return self.monitoring_info


class MemoryReadProcess(process_manager.ProcWorker):
    def __init__(self, input_path, machine_memory_size,
                 conn, machine, result_queue):
        self.input_path = input_path
        self.result_queue = result_queue
        self.machine_memory_size = machine_memory_size*1024
        self.total_read_size = 0
        self.total_write_size = 0
        self.conn = conn
        self.machine = machine

        self.memory_snapshot_size = multiprocessing.Value('d', 0.0)
        self.memory_snapshot_size.value = long(0)
        super(MemoryReadProcess, self).__init__(target=self.read_mem_snapshot)

    def read_mem_snapshot(self):
        # create memory snapshot aligned with 4KB
        time_s = time.time()
        is_first_recv = False
        time_first_recv = 0
        UPDATE_SIZE  = 1024*1024*10 # 10MB
        prev_processed_size = 0
        prev_processed_time = time.time()
        cur_processed_size = 0

        for repeat in xrange(100):
            if os.path.exists(self.input_path) == False:
                print "waiting for %s: " % self.input_path
                time.sleep(0.1)
        try:
            self.in_fd = open(self.input_path, 'rb')
            self.total_read_size = 0
            self.total_write_size = 0
            # read first 40KB and aligen header with 4KB
            data = self.in_fd.read(Memory.Memory.RAM_PAGE_SIZE*10)
            if is_first_recv == False:
                is_first_recv = True
                time_first_recv = time.time()

            libvirt_header = memory_util._QemuMemoryHeaderData(data)
            original_header = libvirt_header.get_header()
            align_size = Const.LIBVIRT_HEADER_SIZE
            new_header = libvirt_header.get_aligned_header(align_size)
            self.result_queue.put(new_header)
            self.total_write_size += len(new_header)

            # get memory snapshot size
            original_header_len = len(original_header)
            memory_size_data = data[original_header_len:original_header_len+Memory.Memory.CHUNK_HEADER_SIZE]
            new_data = data[original_header_len+Memory.Memory.CHUNK_HEADER_SIZE:]
            mem_snapshot_size, = struct.unpack(Memory.Memory.CHUNK_HEADER_FMT, memory_size_data)
            self.memory_snapshot_size.value = long(mem_snapshot_size + len(new_header))
            self.result_queue.put(new_data)
            self.total_write_size += len(new_data)
            LOG.info("Memory snapshot size: %ld, header size: %ld at %f" % \
                     (mem_snapshot_size, len(new_header), time.time()))

            # write rest of the memory data
            #prog_bar = AnimatedProgressBar(end=100, width=80, stdout=sys.stdout)
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
                    self.total_write_size += current_size
                    #prog_bar.set_percent(100.0*self.total_write_size/mem_snapshot_size)
                    #prog_bar.show_progress()

                    if self.total_read_size - prev_processed_size >= UPDATE_SIZE:
                        cur_time = time.time()
                        throughput = float((self.total_read_size-prev_processed_size)/(cur_time-prev_processed_time))
                        prev_processed_size = self.total_read_size
                        prev_processed_time = cur_time
                        self.monitor_current_bw = (throughput/Const.CHUNK_SIZE)
            #prog_bar.finish()
        except Exception, e:
            sys.stdout.write("[MemorySnapshotting] Exception1n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("%s\n" % str(e))
            self.result_queue.put(Const.QUEUE_FAILED_MESSAGE)
        else:
            self.result_queue.put(Const.QUEUE_SUCCESS_MESSAGE)

        time_e = time.time()
        self.is_processing_alive.value = False
        LOG.debug("[time] Memory snapshotting first input at : %f" % (time_first_recv))
        LOG.debug("profiling\t%s\tsize\t%ld\t%ld\t%f" % \
                  (self.__class__.__name__, self.total_write_size, self.total_write_size, 1))
        LOG.debug("profiling\t%s\ttime\t%f\t%f\t%f" % \
                  (self.__class__.__name__, time_s, time_e, (time_e-time_s)))

    def get_memory_snapshot_size(self):
        if long(self.memory_snapshot_size.value) > 0:
            return long(self.memory_snapshot_size.value)
        return long(-1)

    def _terminate_vm(self, conn, machine):
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

    def finish(self):
        if os.path.exists(self.input_path) == True:
            os.remove(self.input_path)
        if self.machine is not None:
            self._terminate_vm(self.conn, self.machine)
            self.machine = None


class LibvirtThread(threading.Thread):
    def __init__(self, machine, outputpath):
        self.machine = machine
        self.outputpath = outputpath
        threading.Thread.__init__(self, target=self.save_mem)

    def save_mem(self):
        self.machine.save(self.outputpath)


class QmpThread(threading.Thread):
    def __init__(self, qmp_path, process_controller, memory_snapshot_queue,
                 compdata_queue, overlay_mode, fuse_stream_monitor):
        self.qmp_path = qmp_path
        self.process_controller = process_controller
        self.memory_snapshot_queue = memory_snapshot_queue
        self.compdata_queue = compdata_queue
        self.overlay_mode = overlay_mode
        self.stop = threading.Event()
        self.qmp = qmp_af_unix.QmpAfUnix(self.qmp_path)
        self.fuse_stream_monitor = fuse_stream_monitor
        self.migration_stop_time = 0
        self.done_configuration = False
        threading.Thread.__init__(self, target=self.control_migration)

    def config_migration(self):
        self.qmp.connect()
        ret = self.qmp.qmp_negotiate()
        if not ret:
            raise HandoffError("failed to connect to qmp channel")
        ret = self.qmp.randomize_raw_live()  # randomize page output order
        if not ret:
            raise HandoffError("failed to randomize memory order")
        LOG.debug("%f\trandomization\tmemory randomization success" % (time.time()))
        self.done_configuration = True

    def control_migration(self):
        LOG.debug("qemu_control\t%f\tstart_thread" % time.time())
        if self.done_configuration is False:
            self.config_migration()

        if VMOverlayCreationMode.LIVE_MIGRATION_STOP == VMOverlayCreationMode.LIVE_MIGRATION_FINISH_ASAP:
            time.sleep(5)
            self.migration_stop_time = self._stop_migration()
        elif VMOverlayCreationMode.LIVE_MIGRATION_STOP == VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE:
            iteration_issued = dict()
            iteration_issue_time_list = list()
            sleep_between_iteration = 2
            loopping_period = 0.1

            # first wait until data is high enough
            loop_counter = 0
            while(not self.stop.wait(loopping_period)):
                unprocessed_memory_snapshot_size = self.memory_snapshot_queue.qsize() *\
                    VMOverlayCreationMode.PIPE_ONE_ELEMENT_SIZE
                if loop_counter % 100 == 0:
                    LOG.debug("qemu_control\tdata check: %s" % (unprocessed_memory_snapshot_size))
                loop_counter += 1
                if unprocessed_memory_snapshot_size > 1024*1024*1: # 10 MB
                    time.sleep(1)
                    break

            LOG.debug("qemu_control\tfinish data checking: %s" % (unprocessed_memory_snapshot_size))
            loop_counter = 0
            while(not self.stop.wait(loopping_period)):
                iter_num = self.process_controller.get_migration_iteration_count()
                unprocessed_memory_snapshot_size = self.memory_snapshot_queue.qsize() *\
                    VMOverlayCreationMode.PIPE_ONE_ELEMENT_SIZE

                # issue new iteration
                is_new_request_issued = False
                if loop_counter % 100 == 0:
                    LOG.debug("qemu_control\tdata left: %s" % (unprocessed_memory_snapshot_size))
                loop_counter += 1
                if unprocessed_memory_snapshot_size < 1024*1024*10: # 10 MB
                    LOG.debug("qemu_control\t%f\tready to request new iteration %d (data: %d)" % \
                              (time.time(), (iter_num+1), unprocessed_memory_snapshot_size))
                    is_iter_requested = iteration_issued.get(iter_num, False)
                    if is_iter_requested is False:
                        # request new iteration
                        LOG.debug("qemu_control\t%f\trequest new iteration %d\t" % \
                                  (time.time(), iter_num+1))
                        ret = self.qmp.iterate_raw_live()
                        iteration_issued[iter_num] = True
                        iteration_issue_time_list.append(time.time())
                        time.sleep(sleep_between_iteration)
                        is_new_request_issued = True
                    else:
                        # already requested, but no more data to process
                        LOG.debug("qemu_control\t%f\trequest overlapped iteration %d\t" % \
                                  (time.time(), iter_num+1))
                        iteration_issue_time_list.append(time.time())
                        time.sleep(sleep_between_iteration)
                        is_new_request_issued = True

                if is_new_request_issued == False or (len(iteration_issue_time_list) < 2):
                    continue

                # check end condition
                lastest_time_diff = iteration_issue_time_list[-1] - iteration_issue_time_list[-2]
                threshold = (loopping_period + sleep_between_iteration)*1.5
                LOG.debug("qemu_control\t%f\tin_data_size:%d\ttime_between_iter:%f (<%f)" % \
                          (time.time(), unprocessed_memory_snapshot_size,
                           lastest_time_diff, threshold))
                if lastest_time_diff < threshold:
                    LOG.debug("qemu_control\t%f\toutput_queue_size:%d\titer_count:%d" %\
                              (time.time(),
                               self.compdata_queue.qsize(), len(iteration_issue_time_list)))
                    if self.compdata_queue.qsize() == 0:
                        # stop after transmitting everything
                        self.migration_stop_time = self._stop_migration()
                        break
                    if len(iteration_issue_time_list) >= 5:
                        self.migration_stop_time = self._stop_migration()
                        break

        self.qmp.disconnect()

    def _stop_migration(self):
        #LOG.debug("qemu_control\tsent stop_raw_live signal at %f" % time.time())
        stop_time = self.qmp.stop_raw_live()
        #LOG.debug("qemu_control\tstop migration at %f" % stop_time)
        self.fuse_stream_monitor.terminate()
        return stop_time

    def _waiting(self, timeout):
        for index in range(timeout):
            sys.stdout.write("waiting %d/%d seconds\n" % (index, timeout))
            time.sleep(1)

    def terminate(self):
        self.stop.set()


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



class StreamSynthesisFile(multiprocessing.Process):
    def __init__(self, basevm_uuid, compdata_queue, temp_compfile_dir):
        self.basevm_uuid = basevm_uuid
        self.compdata_queue = compdata_queue
        self.temp_compfile_dir = temp_compfile_dir
        self.manager = multiprocessing.Manager()
        self.overlay_info = self.manager.list()
        self.overlay_files = self.manager.list()
        super(StreamSynthesisFile, self).__init__(target=self.save_to_file)

    def get_overlay_info(self):
        overlay_info_list = list()
        for overlay_item_dict in self.overlay_info:
            item_dict = dict()
            for key, value in overlay_item_dict.iteritems():
                item_dict[key] = value
            overlay_info_list.append(item_dict)
        return overlay_info_list, self.overlay_files

    def save_to_file(self):
        #is_first_recv_disk = False
        #is_first_recv_memory = False
        comp_file_counter = 0
        input_fd = [self.compdata_queue._reader.fileno()]
        while True:
            input_ready, out_ready, err_ready = select.select(input_fd, [], [])
            if self.compdata_queue._reader.fileno() in input_ready:
                comp_task = self.compdata_queue.get()
                time_process_start = time.time()
                if comp_task == Const.QUEUE_SUCCESS_MESSAGE:
                    break
                if comp_task == Const.QUEUE_FAILED_MESSAGE:
                    LOG.error("Failed to get compressed data")
                    break
                (blob_comp_type, compdata, disk_chunks, memory_chunks) = comp_task
                blob_filename = os.path.join(self.temp_compfile_dir, "%s-stream-%d" %\
                                            (Const.OVERLAY_FILE_PREFIX,
                                            comp_file_counter))
                #if is_first_recv_disk == False and len(disk_chunks) > 0:
                #    is_first_recv_disk = True
                #    LOG.debug("[time] Transfer first disk input at : %f (disk:%d, memory:%d)" % \
                #              (time_process_start, len(disk_chunks), len(memory_chunks)))
                #if is_first_recv_memory == False and len(memory_chunks) > 0:
                #    is_first_recv_memory = True
                #    LOG.debug("[time] Transfer first memory input at : %f (disk:%d, memory:%d)" % \
                #              (time_process_start, len(disk_chunks), len(memory_chunks)))
                #LOG.debug("%s --> %d" % (blob_filename, blob_comp_type))
                #LOG.debug("%s: # of delta memory: %d\t# of delta disk: %d" %\
                #          (blob_filename, len(memory_chunks), len(disk_chunks)))
                comp_file_counter += 1
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
                self.overlay_files.append(blob_filename)
                self.overlay_info.append(blob_dict)
                time_process_end = time.time()

                # wait to emulate network badwidth
                processed_time = time_process_end-time_process_start
                processed_size = os.path.getsize(blob_filename)
                emulated_time = (processed_size*8) / (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps*1024.0*1024)
                if emulated_time > processed_time:
                    sleep_time = (emulated_time-processed_time)
                    #LOG.debug("Emulating BW of %d Mbps, so wait %f s" %\ (VMOverlayCreationMode.EMULATED_BANDWIDTH_Mbps, sleep_time))
                    time.sleep(sleep_time)


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
    m_chunk_queue = getattr(monitoring_info, INFO.DISK_MODIFIED_BLOCKS, dict())
    trim_dict = getattr(monitoring_info, INFO.DISK_FREE_BLOCKS, None)
    used_blocks_dict = getattr(monitoring_info, INFO.DISK_USED_BLOCKS, None)
    dma_dict = dict()
    apply_discard = True

    LOG.info("Get memory delta")
    time_s = time.time()

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
        if overlay_mode.PROCESS_PIPELINED == False:
            _waiting_to_finish(process_controller, "CreateMemoryDeltalist")
        time_mem_delta = time.time()

    LOG.info("Get disk delta")
    disk_deltalist_queue = multiprocessing.Queue(maxsize=overlay_mode.QUEUE_SIZE_DISK_DELTA_LIST)
    disk_deltalist_proc = Disk.CreateDiskDeltalist(modified_disk,
                                                   m_chunk_queue,
                                                   Const.CHUNK_SIZE,
                                                   disk_deltalist_queue,
                                                   base_image,
                                                   overlay_mode,
                                                   trim_dict,
                                                   dma_dict,
                                                   apply_discard,
                                                   used_blocks_dict)
    disk_deltalist_proc.start()
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "CreateDiskDeltalist")
    time_disk_delta = time.time()

    LOG.info("Generate VM overlay using deduplication")
    dedup_proc = delta.DeltaDedup(memory_deltalist_queue, Memory.Memory.RAM_PAGE_SIZE,
                                  disk_deltalist_queue, Const.CHUNK_SIZE,
                                  merged_deltalist_queue,
                                  overlay_mode,
                                  basedisk_hashdict=basedisk_hashdict,
                                  basemem_hashdict=basemem_hashdict)
    dedup_proc.start()
    time_merge_delta = time.time()

    #LOG.info("Print statistics")
    #disk_deltalist_proc.join()   # to fill out disk_statistics
    #disk_statistics = disk_deltalist_proc.ret_statistics
    #free_memory_dict = getattr(monitoring_info, _MonitoringInfo.MEMORY_FREE_BLOCKS, None)
    #free_pfn_counter = long(free_memory_dict.get("freed_counter", 0))
    #disk_discarded_count = disk_statistics.get('trimed', 0)
    #DeltaList.statistics(merged_deltalist,
    #        mem_discarded=free_pfn_counter,
    #        disk_discarded=disk_discarded_count)
    time_e = time.time()
    LOG.debug("Total time for getting deltalist: %f" % (time_e-time_s))
    LOG.debug("  memory deltalist: %f" % (time_mem_delta-time_s))
    LOG.debug("  disk deltalist: %f" % (time_disk_delta-time_mem_delta))
    LOG.debug("  merge deltalist: %f" % (time_merge_delta-time_disk_delta))
    return dedup_proc



def save_mem_snapshot(conn, machine, output_queue, **kwargs):
    #Set migration speed
    nova_util = kwargs.get('nova_util', None)
    fuse_stream_monitor = kwargs.get('fuse_stream_monitor', None)
    ret = machine.migrateSetMaxSpeed(1000000, 0)   # 1000 Gbps, unlimited
    if ret != 0:
        raise HandoffError("Cannot set migration speed : %s", machine.name())

    # Stop monitoring for memory access (snapshot will create a lot of access)
    fuse_stream_monitor.del_path(cloudletfs.StreamMonitor.MEMORY_ACCESS)
    #if fuse_stream_monitor is not None:
    #    fuse_stream_monitor.terminate()
    #    fuse_stream_monitor.join()

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
            raise HandoffError("libvirt memory save : " + str(e))
    finally:
        pass

    if ret != 0:
        raise HandoffError("libvirt: Cannot save memory state")

    return memory_read_proc


def _waiting_to_finish(process_controller, worker_name):
    while True:
        worker_info = process_controller.process_infos.get(worker_name, None)
        if worker_info == None:
            raise HandoffError("Failed to access %s worker" % worker_name)
        if worker_info['is_processing_alive'].value == False:
            break
        else:
            time.sleep(0.01)


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


class CPUMonitor(threading.Thread):
    def __init__(self):
        self.cpu_percent_list = list()
        self.stop = threading.Event()
        threading.Thread.__init__(self, target=self.monitor_cpu)

    def getCPUUsage(self):
        return self.core_cpus

    def monitor_cpu(self):
        import psutil
        while(not self.stop.wait(1)):
            cpu_usage = psutil.cpu_percent(interval=0, percpu=True)
            cur_time = time.time()
            self.cpu_percent_list.append((cur_time, cpu_usage))
            #LOG.debug("cpu_usage\t%f\t%s" % (cur_time, str(cpu_usage)))

    def average_cpu_time(self, start_time, end_time, assigned_core_list):
        core_cpus = {}.fromkeys(range(8), 0)
        count_datapoint = 0

        # sum across datapoints
        for (measured_time, cpu_percent) in self.cpu_percent_list:
            if measured_time < start_time:
                #LOG.debug("cpu_usage\tbefore start processing: %s" % cpu_percent)
                continue
            if measured_time > end_time:
                #LOG.debug("cpu_usage\tafter finish processing: %s" % cpu_percent)
                continue
            for index, each_core in enumerate(cpu_percent):
                core_cpus[index] += each_core
            count_datapoint += 1
        for index in core_cpus:
            core_cpus[index] = core_cpus[index]/count_datapoint
        LOG.debug("cpu_usage\t%f\t%s" % (time.time(), str(core_cpus)))

        avg_cpu_percent = 0.0
        for core_index in assigned_core_list:
            avg_cpu_percent += core_cpus[core_index]
        return avg_cpu_percent/len(assigned_core_list)

    def terminate(self):
        self.stop.set()


def create_residue(base_disk, base_hashvalue,
                   basedisk_hashdict, basemem_hashdict,
                   resumed_vm, options, original_deltalist,
                   migration_addr,
                   overlay_mode=None):
    '''Get residue
    return overlay_metafile, overlay_files
    '''
    import psutil
    global _handoff_start_time  # for testing purpose
    time_start = time.time()
    _handoff_start_time[0] = time_start
    LOG.info("control_network\tupdate start time: %f" % _handoff_start_time[0])

    CPU_MONITORING = False
    if CPU_MONITORING:
        cpu_stat_start = psutil.cpu_times(percpu = True)
    process_controller = process_manager.get_instance()
    if overlay_mode == None:
        NUM_CPU_CORES = 1   # set CPU affinity
        VMOverlayCreationMode.LIVE_MIGRATION_STOP = VMOverlayCreationMode.LIVE_MIGRATION_FINISH_USE_SNAPSHOT_SIZE
        overlay_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue(num_cores=NUM_CPU_CORES)
        overlay_mode.COMPRESSION_ALGORITHM_TYPE = Const.COMPRESSION_GZIP
        overlay_mode.COMPRESSION_ALGORITHM_SPEED = 1
        overlay_mode.MEMORY_DIFF_ALGORITHM = "none"
        overlay_mode.DISK_DIFF_ALGORITHM = "none"

    # set affinity of VM not to disturb the migration
    # do this after creating overlay mode
    core_index = VMOverlayCreationMode.get_cpu_core_index()
    assigned_core_list = VMOverlayCreationMode.get_cpu_core_index()
    excluded_core_list = list(set(range(0,8)) - set(assigned_core_list))
    for proc in psutil.process_iter():
        if proc.name.lower().startswith("cloudlet_"):
            proc.set_cpu_affinity(excluded_core_list)
            LOG.debug("affinity\tset affinity of %s to %s" % (proc.name, excluded_core_list))

    process_controller.set_mode(overlay_mode, migration_addr)
    LOG.info("* LIVE MIGRATION STRATEGY: %d" % VMOverlayCreationMode.LIVE_MIGRATION_STOP)
    LOG.info("* Overlay creation configuration")
    LOG.info("  - %s" % str(options))
    LOG.debug("* Overlay creation mode start\n%s" % str(overlay_mode))
    LOG.debug("* Overlay creation mode end")

    # sanity check
    if (options == None) or (isinstance(options, Options) == False):
        raise HandoffError("Given option is invalid: %s" % str(options))
    (base_diskmeta, base_mem, base_memmeta) = \
            Const.get_basepath(base_disk, check_exist=True)
    qemu_logfile = resumed_vm.qemu_logfile

    # start CPU Monitor
    if CPU_MONITORING:
        cpu_monitor = CPUMonitor()
        cpu_monitor.start()

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
    time_ss = time.time()
    LOG.debug("[time] serialized step (%f ~ %f): %f" % (time_start,
                                                        time_ss,
                                                        (time_ss-time_start)))
    if (time_ss-time_start) > 5:
        raise HandoffError("Time for serialized step takes too long. Check get_monitoring_info()")

    # QEMU control thread
    qmp_thread = QmpThread(resumed_vm.qmp_channel, process_controller,
                           memory_snapshot_queue, compdata_queue,
                           overlay_mode, resumed_vm.monitor)
    qmp_thread.daemon = True
    qmp_thread.config_migration()

    # memory snapshotting thread
    memory_read_proc = save_mem_snapshot(resumed_vm.conn,
                                         resumed_vm.machine,
                                         memory_snapshot_queue,
                                         fuse_stream_monitor=resumed_vm.monitor)
    if overlay_mode.PROCESS_PIPELINED == False:
        if overlay_mode.LIVE_MIGRATION_STOP is not VMOverlayCreationMode.LIVE_MIGRATION_FINISH_ASAP:
            msg = "Use ASAP VM stop for pipelined approach for serialized processing.\n"
            msg += "Otherwise it won't fininsh at the memory dumping stage"
            raise HandoffError(msg)
        time.sleep(5)
        qmp_thread.start()
        _waiting_to_finish(process_controller, "MemoryReadProcess")

    # process for getting VM overlay
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
    time_dedup = time.time()
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "DeltaDedup")

    # process for compression
    LOG.info("Compressing overlay blobs")
    compress_proc = compression.CompressProc(residue_deltalist_queue,
                                             compdata_queue,
                                             overlay_mode)
    compress_proc.start()
    time_dedup = time.time()
    if overlay_mode.PROCESS_PIPELINED == False:
        _waiting_to_finish(process_controller, "CompressProc")

    if migration_addr.startswith("network"):
        from stream_client import StreamSynthesisClient
        migration_dest_ip = migration_addr.split(":")[-1]
        resume_disk_size = os.path.getsize(resumed_vm.resumed_disk)

        # wait until getting the memory snapshot size
        resume_memory_size = -1
        LOG.debug("waiting to get memory size")
        while resume_memory_size < 0:
            resume_memory_size = memory_read_proc.get_memory_snapshot_size()
        time_memory_snapshot_size = time.time()
        LOG.debug("[time] Getting memory snapshot size (%f~%f):%f" % (time_start,
                                                                        time_memory_snapshot_size,
                                                                        (time_memory_snapshot_size-time_start)))
        if overlay_mode.PROCESS_PIPELINED == True:
            qmp_thread.start()

        metadata = dict()
        metadata[Const.META_BASE_VM_SHA256] = base_hashvalue
        metadata[Const.META_RESUME_VM_DISK_SIZE] = resume_disk_size
        metadata[Const.META_RESUME_VM_MEMORY_SIZE] = resume_memory_size
        time_network_start = time.time()
        client = StreamSynthesisClient(migration_dest_ip, metadata, compdata_queue)
        client.start()
        client.join()
        cpu_stat_end = psutil.cpu_times(percpu=True)
        time_network_end = time.time()
        LOG.debug("[time] Network transmission (%f~%f):%f" % (time_network_start,
                                                              time_network_end,
                                                              (time_network_end-time_network_start)))
        cpu_stat = process_controller.cpu_statistics
        process_manager.kill_instance()

        # 7. terminting
        if resumed_vm.monitor is not None:
            resumed_vm.monitor.terminate()
            resumed_vm.monitor.join()
        resumed_vm.machine = None   # protecting malaccess to machine 
        time_end = time.time()

        qmp_thread.join()
        migration_stop_command_time = qmp_thread.migration_stop_time
        vm_resume_time_at_dest = client.vm_resume_time_at_dest.value
        time_finish_transmission = client.time_finish_transmission.value
        LOG.debug("[time] migration stop time: %f" % migration_stop_command_time)
        LOG.debug("[time] VM resume time at dest: %f" % vm_resume_time_at_dest)
        LOG.debug("[time] migration downtime: %f" % (vm_resume_time_at_dest-migration_stop_command_time))
        LOG.debug("[time] Start ~ Finish tranmission (%f ~ %f): %f" % (time_start, time_finish_transmission,
                                                                (time_finish_transmission-time_start)))
        LOG.debug("[time] Start ~ Finish migration (%f ~ %f): %f" % (time_start, vm_resume_time_at_dest,
                                                                (vm_resume_time_at_dest-time_start)))
        if CPU_MONITORING:
            # measure CPU usage
            cpu_monitor.terminate()
            cpu_monitor.join()
            avg_cpu_usage = cpu_monitor.average_cpu_time(time_start, time_finish_transmission, assigned_core_list)
            LOG.debug("cpu_usage\t%f\taverage\t%s" % (time.time(), avg_cpu_usage))
            # measrue CPU time
            cpu_user_time = 0.0
            cpu_sys_time = 0.0
            cpu_idle_time = 0.0
            for core_index in assigned_core_list:
                cpu_time_start = cpu_stat_start[core_index]
                cpu_time_end = cpu_stat_end[core_index]
                cpu_user_time += (cpu_time_end[0] - cpu_time_start[0])
                cpu_sys_time += (cpu_time_end[2] - cpu_time_start[2])
                cpu_idle_time += (cpu_time_end[3] - cpu_time_start[3])
            cpu_total_time = cpu_user_time+cpu_sys_time
            LOG.debug("cpu_usage\t%f\tostime\t%s\t%f\t%f %%(not accurate)" %\
                    (time.time(), assigned_core_list,
                    cpu_total_time, 100.0*cpu_total_time/(cpu_total_time+cpu_idle_time)))

        _handoff_start_time[0] = sys.maxint
        return None
    elif migration_addr.startswith("file"):
        temp_compfile_dir = mkdtemp(prefix="cloudlet-comp-")
        synthesis_file = StreamSynthesisFile(base_hashvalue, compdata_queue, temp_compfile_dir)
        synthesis_file.start()

        # wait until getting the memory snapshot size
        LOG.debug("waiting to get memory size")
        resume_memory_size = -1
        while resume_memory_size < 0:
            resume_memory_size = memory_read_proc.get_memory_snapshot_size()
            time.sleep(0.001)
        time_memory_snapshot_size = time.time()
        LOG.debug("[time] Getting memory snapshot size (%f~%f):%f" % (time_start,
                                                                        time_memory_snapshot_size,
                                                                        (time_memory_snapshot_size-time_start)))
        if overlay_mode.PROCESS_PIPELINED == True:
            qmp_thread.start()

        # wait to finish creating files
        synthesis_file.join()
        time_end_transfer = time.time()
        LOG.debug("[time] Time for finishing transferring (%f ~ %f): %f" % (time_start,
                                                                            time_end_transfer,
                                                                            (time_end_transfer-time_start)))

        overlay_info, overlay_files = synthesis_file.get_overlay_info()
        overlay_metapath = os.path.join(os.getcwd(), Const.OVERLAY_META)
        overlay_metafile = _generate_overlaymeta(overlay_metapath,
                                                overlay_info,
                                                base_hashvalue,
                                                os.path.getsize(resumed_vm.resumed_disk),
                                                resume_memory_size)

        # packaging VM overlay into a single zip file
        temp_dir = mkdtemp(prefix="cloudlet-overlay-")
        overlay_zipfile = os.path.join(temp_dir, Const.OVERLAY_ZIP)
        VMOverlayPackage.create(overlay_zipfile, overlay_metafile, overlay_files)

        # terminting
        qmp_thread.join()
        process_manager.kill_instance()
        memory_read_proc.finish()   # deallocate resources for snapshotting
        # 7. terminting
        if resumed_vm.monitor is not None:
            resumed_vm.monitor.terminate()
            resumed_vm.monitor.join()
        resumed_vm.machine = None   # protecting malaccess to machine 
        if os.path.exists(overlay_metafile) == True:
            os.remove(overlay_metafile)
        if os.path.exists(temp_compfile_dir) == True:
            shutil.rmtree(temp_compfile_dir)
        time_end = time.time()
        LOG.debug("[time] Total residue creation time (%f ~ %f): %f" % (time_start, time_end,
                                                                (time_end-time_start)))

        if CPU_MONITORING:
            cpu_monitor.terminate()
            cpu_monitor.join()
            avg_cpu_usage = cpu_monitor.average_cpu_time(time_start, time_end_transfer, assigned_core_list)
            LOG.debug("cpu_usage\t%f\taverage\t%s" % (time.time(), avg_cpu_usage))
        _handoff_start_time[0] = sys.maxint
        return overlay_zipfile

