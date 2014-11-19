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

import os
import traceback
import sys
import time
import struct
import SocketServer
import socket
import tempfile

from server import NetworkUtil
from synthesis_protocol import Protocol as Protocol

#import synthesis as synthesis
#from package import VMOverlayPackage
from db.api import DBConnector
from db.table_def import BaseVM
from Configuration import Const as Cloudlet_Const
#from Configuration import Synthesis_Const as Synthesis_Const
#import msgpack
#
#from pprint import pformat
from optparse import OptionParser
#from multiprocessing import Process, JoinableQueue, Queue, Manager
#from lzma import LZMADecompressor
import log as logging


LOG = logging.getLogger(__name__)
session_resources = dict()   # dict[session_id] = obj(SessionResource)


class StreamSynthesisError(Exception):
    pass

class StreamSynthesisHandler(SocketServer.StreamRequestHandler):
    synthesis_option = {
            Protocol.SYNTHESIS_OPTION_DISPLAY_VNC : False,
            Protocol.SYNTHESIS_OPTION_EARLY_START : False,
            Protocol.SYNTHESIS_OPTION_SHOW_STATISTICS : False
            }

    def ret_fail(self, message):
        LOG.error("%s" % str(message))
        message = NetworkUtil.encoding({
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_FAIELD,
            Protocol.KEY_FAILED_REASON : message
            })
        message_size = struct.pack("!I", len(message))
        self.request.send(message_size)
        self.wfile.write(message)

    def ret_success(self, req_command, payload=None):
        send_message = {
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_SUCCESS,
            Protocol.KEY_REQUESTED_COMMAND : req_command,
            }
        if payload:
            send_message.update(payload)
        message = NetworkUtil.encoding(send_message)
        message_size = struct.pack("!I", len(message))
        self.request.send(message_size)
        self.wfile.write(message)
        self.wfile.flush()

    def send_synthesis_done(self):
        message = NetworkUtil.encoding({
            Protocol.KEY_COMMAND : Protocol.MESSAGE_COMMAND_SYNTHESIS_DONE,
            })
        LOG.info("SUCCESS to launch VM")
        try:
            message_size = struct.pack("!I", len(message))
            self.request.send(message_size)
            self.wfile.write(message)
        except socket.error as e:
            pass

    def _check_validity(self, message):
        header_info = None
        requested_base = None

        if (message.get(Protocol.KEY_META_SIZE, 0) > 0):
            # check header option
            client_syn_option = message.get(Protocol.KEY_SYNTHESIS_OPTION, None)
            if client_syn_option != None and len(client_syn_option) > 0:
                self.synthesis_option.update(client_syn_option)

            # receive overlay meta file
            meta_file_size = message.get(Protocol.KEY_META_SIZE)
            header_data = self.request.recv(meta_file_size)
            while len(header_data) < meta_file_size:
                header_data += self.request.recv(meta_file_size- len(header_data))
            header = NetworkUtil.decoding(header_data)
            base_hashvalue = header.get(Cloudlet_Const.META_BASE_VM_SHA256, None)

            # check base VM
            for each_basevm in self.server.basevm_list:
                if base_hashvalue == each_basevm.hash_value:
                    LOG.info("New client request %s VM" \
                            % (each_basevm.disk_path))
                    requested_base = each_basevm.disk_path
                    header_info = header
        return [requested_base, header_info]


    def _handle_finish(self, message):
        '''
        global session_resources

        session_id = message.get(Protocol.KEY_SESSION_ID, None)
        session_resource = session_resources.get(session_id)
        if session_resource is None:
            # No saved resource for the session
            msg = "No resource to be deallocated found at Session (%s)" % session_id
            LOG.warning(msg)
        else:
            # deallocate all the session resource
            msg = "Deallocating resources for the Session (%s)" % session_id
            LOG.info(msg)
            session_resource.deallocate()
            del session_resources[session_id]

        LOG.info("  - %s" % str(pformat(message)))
        self.ret_success(Protocol.MESSAGE_COMMAND_FINISH)
        '''

    def _check_url_validity(self, message):
        requested_base = None
        metadata = None
        try:
            # check header option
            client_syn_option = message.get(Protocol.KEY_SYNTHESIS_OPTION, None)
            if client_syn_option != None and len(client_syn_option) > 0:
                self.synthesis_option.update(client_syn_option)

            # receive overlay meta file
            overlay_url = message.get(Protocol.KEY_OVERLAY_URL)
            overlay_package = VMOverlayPackage(overlay_url)
            metadata = NetworkUtil.decoding(overlay_package.read_meta())
            base_hashvalue = metadata.get(Cloudlet_Const.META_BASE_VM_SHA256, None)

            # check base VM
            for each_basevm in self.server.basevm_list:
                if base_hashvalue == each_basevm.hash_value:
                    LOG.info("New client request %s VM" \
                            % (each_basevm.disk_path))
                    requested_base = each_basevm.disk_path
        except Exception, e:
            pass
        return [requested_base, metadata]

    def _handle_synthesis_url(self, message):
        '''
        LOG.info("\n\n----------------------- New Connection --------------")
        # check overlay meta info
        start_time = time.time()
        header_start_time = time.time()
        base_path, meta_info = self._check_url_validity(message)
        if meta_info is None:
            self.ret_fail("cannot access overlay URL")
            return
        if base_path is None:
            self.ret_fail("No matching Base VM")
            return
        if meta_info.get(Cloudlet_Const.META_OVERLAY_FILES, None) is None:
            self.ret_fail("No overlay files are listed")
            return 

        # return success get overlay URL
        self.ret_success(Protocol.MESSAGE_COMMAND_SEND_META)
        overlay_url = message.get(Protocol.KEY_OVERLAY_URL)
        overlay_package = VMOverlayPackage(overlay_url)

        # update DB
        session_id = message.get(Protocol.KEY_SESSION_ID, None)
        new_overlayvm = OverlayVM(session_id, base_path)
        self.server.dbconn.add_item(new_overlayvm)

        # start synthesis process
        url_manager = Manager()
        overlay_urls = url_manager.list()
        overlay_urls_size = url_manager.dict()
        for blob in meta_info[Cloudlet_Const.META_OVERLAY_FILES]:
            url = blob[Cloudlet_Const.META_OVERLAY_FILE_NAME]
            size = blob[Cloudlet_Const.META_OVERLAY_FILE_SIZE]
            overlay_urls.append(url)
            overlay_urls_size[url] = size
        LOG.info("  - %s" % str(pformat(self.synthesis_option)))
        LOG.info("  - Base VM     : %s" % base_path)
        LOG.info("  - Blob count  : %d" % len(overlay_urls))
        if overlay_urls == None:
            self.ret_fail("No overlay info listed")
            return
        (base_diskmeta, base_mem, base_memmeta) = \
                Cloudlet_Const.get_basepath(base_path, check_exist=True)
        header_end_time = time.time()
        LOG.info("Meta header processing time: %f" % (header_end_time-header_start_time))

        # read overlay files
        # create named pipe to convert queue to stream
        time_transfer = Queue(); time_decomp = Queue();
        time_delta = Queue(); time_fuse = Queue();
        self.tmp_overlay_dir = tempfile.mkdtemp()
        self.overlay_pipe = os.path.join(self.tmp_overlay_dir, 'overlay_pipe')
        os.mkfifo(self.overlay_pipe)

        # save overlay decomp result for measurement
        temp_overlay_file = None
        if self.synthesis_option.get(Protocol.SYNTHESIS_OPTION_SHOW_STATISTICS):
            temp_overlay_filepath = os.path.join(self.tmp_overlay_dir, "overlay_file")
            temp_overlay_file = open(temp_overlay_filepath, "w+b")

        # overlay
        demanding_queue = Queue()
        download_queue = JoinableQueue()
        download_process = URLFetchStep(overlay_package, overlay_urls, 
                overlay_urls_size, demanding_queue, download_queue, 
                time_transfer, Synthesis_Const.TRANSFER_SIZE, )
        decomp_process = DecompStepProc(
                download_queue, self.overlay_pipe, time_decomp, temp_overlay_file,
                )
        modified_img, modified_mem, self.fuse, self.delta_proc, self.fuse_proc = \
                synthesis.recover_launchVM(base_path, meta_info, self.overlay_pipe, 
                        log=sys.stdout, demanding_queue=demanding_queue)
        self.delta_proc.time_queue = time_delta # for measurement
        self.fuse_proc.time_queue = time_fuse # for measurement

        if self.synthesis_option.get(Protocol.SYNTHESIS_OPTION_EARLY_START, False):
            # 1. resume VM
            self.resumed_VM = synthesis.SynthesizedVM(modified_img, modified_mem, self.fuse)
            time_start_resume = time.time()
            self.resumed_VM.start()
            time_end_resume = time.time()

            # 2. start processes
            download_process.start()
            decomp_process.start()
            self.delta_proc.start()
            self.fuse_proc.start()

            # 3. return success right after resuming VM
            # before receiving all chunks
            self.resumed_VM.join()
            self.send_synthesis_done()

            # 4. then wait fuse end
            self.fuse_proc.join()
        else:
            # 1. start processes
            download_process.start()
            decomp_process.start()
            self.delta_proc.start()
            self.fuse_proc.start()

            # 2. resume VM
            self.resumed_VM = synthesis.SynthesizedVM(modified_img, modified_mem, self.fuse)
            self.resumed_VM.start()

            # 3. wait for fuse end
            self.fuse_proc.join()

            # 4. return success to client
            time_start_resume = time.time()     # measure pure resume time
            self.resumed_VM.join()
            time_end_resume = time.time()
            self.send_synthesis_done()

        end_time = time.time()

        # printout result
        SynthesisHandler.print_statistics(start_time, end_time, \
                time_transfer, time_decomp, time_delta, time_fuse, \
                resume_time=(time_end_resume-time_start_resume))

        if self.synthesis_option.get(Protocol.SYNTHESIS_OPTION_DISPLAY_VNC, False):
            synthesis.connect_vnc(self.resumed_VM.machine, no_wait=True)

        # save all the resource to the session resource
        global session_resources
        s_resource = SessionResource(session_id)
        s_resource.add(SessionResource.DELTA_PROCESS, self.delta_proc)
        s_resource.add(SessionResource.RESUMED_VM, self.resumed_VM)
        s_resource.add(SessionResource.FUSE, self.fuse)
        s_resource.add(SessionResource.OVERLAY_PIPE, self.overlay_pipe)
        s_resource.add(SessionResource.OVERLAY_DIR, self.tmp_overlay_dir)
        s_resource.add(SessionResource.OVERLAY_DB_ENTRY, new_overlayvm)
        session_resources[session_id] = s_resource
        LOG.info("Resource is allocated for Session: %s" % str(session_id))

        # printout synthesis statistics
        if self.synthesis_option.get(Protocol.SYNTHESIS_OPTION_SHOW_STATISTICS):
            mem_access_list = self.resumed_VM.monitor.mem_access_chunk_list
            disk_access_list = self.resumed_VM.monitor.disk_access_chunk_list
            synthesis.synthesis_statistics(meta_info, temp_overlay_filepath, \
                    mem_access_list, disk_access_list)
        LOG.info("[SOCKET] waiting for client exit message")
        '''
        pass

    def _recv_all(self, recv_size):
        data = ''
        while len(data) < recv_size:
            tmp_data = self.request.recv(recv_size - len(data))
            if tmp_data == None:
                raise StreamSynthesisError("Cannot recv data at %s" % str(self))
            if len(tmp_data) == 0:
                raise StreamSynthesisError("Recv 0 data at %s" % str(self))
            data += tmp_data
        return data

    def handle(self):
        '''Handle request from the client
        Each request follows this format: 

        | header size | header | blob header size | blob header | blob data  |
        |  (4 bytes)  | (var)  | (4 bytes)        | (var bytes) | (var bytes)|
        '''

        # get header
        data = self.request.recv(4)
        if data == None or len(data) != 4:
            raise StreamSynthesisError("Failed to receive first byte of header")
        message_size = struct.unpack("!I", data)[0]
        msgpack_data = self._recv_all(message_size)
        message = NetworkUtil.decoding(msgpack_data)
        basevm_uuid = message.get("basevm_uuid", None)

        # get each blob
        import pdb;pdb.set_trace()
        while True:
            data = self.request.recv(4)
            if data == None or len(data) != 4:
                raise StreamSynthesisError("Failed to receive first byte of header")
                break
            blob_header_size = struct.unpack("!I", data)[0]
            blob_header_raw = self._recv_all(blob_header_size)
            blob_header = NetworkUtil.decoding(blob_header_raw)
            blob_size = blob_header.get(Cloudlet_Const.META_OVERLAY_FILE_SIZE)
            if blob_size == None:
                raise StreamSynthesisError("Failed to receive blob")
            if blob_size == 0:
                print "end of stream"
                break
            compressed_blob = self._recv_all(blob_size)

    def finish(self):
        pass

    def terminate(self):
        # force terminate when something wrong in handling request
        # do not wait for joinining
        if hasattr(self, 'detla_proc') and self.delta_proc != None:
            self.delta_proc.finish()
            if self.delta_proc.is_alive():
                self.delta_proc.terminate()
            self.delta_proc = None
        if hasattr(self, 'resumed') and self.resumed_VM != None:
            self.resumed_VM.terminate()
            self.resumed_VM = None
        if hasattr(self, 'fuse') and self.fuse != None:
            self.fuse.terminate()
            self.fuse = None
        if hasattr(self, 'overlay_pipe') and os.path.exists(self.overlay_pipe):
            os.unlink(self.overlay_pipe)
        if hasattr(self, 'tmp_overlay_dir') and os.path.exists(self.tmp_overlay_dir):
            shutil.rmtree(self.tmp_overlay_dir)

def get_local_ipaddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    ipaddress = (s.getsockname()[0])
    s.close()
    return ipaddress


class StreamSynthesisConst(object):
    SERVER_PORT_NUMBER = 8022
    VERSION = 0.1


class StreamSynthesisServer(SocketServer.TCPServer):
    def __init__(self, args):
        settings, args = StreamSynthesisServer.process_command_line(args)
        self.dbconn = DBConnector()
        self.basevm_list = self.check_basevm()

        StreamSynthesisConst.LOCAL_IPADDRESS = "0.0.0.0"
        server_address = (StreamSynthesisConst.LOCAL_IPADDRESS, StreamSynthesisConst.SERVER_PORT_NUMBER)

        self.allow_reuse_address = True
        try:
            SocketServer.TCPServer.__init__(self, server_address, StreamSynthesisHandler)
        except socket.error as e:
            sys.stderr.write(str(e))
            sys.stderr.write("Check IP/Port : %s\n" % (str(server_address)))
            sys.exit(1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        LOG.info("* Server configuration")
        LOG.info(" - Open TCP Server at %s" % (str(server_address)))
        LOG.info(" - Disable Nagle(No TCP delay)  : %s" \
                % str(self.socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)))
        LOG.info("-"*50)

    def handle_error(self, request, client_address):
        #SocketServer.TCPServer.handle_error(self, request, client_address)
        #sys.stderr.write("handling error from client %s\n" % (str(client_address)))
        pass

    def terminate(self):
        # close all thread
        if self.socket != -1:
            self.socket.close()

        global session_resources
        for (session_id, resource) in session_resources.iteritems():
            try:
                resource.deallocate()
                msg = "Deallocate resources for Session: %s" % str(session_id)
                LOG.info(msg)
            except Exception as e:
                msg = "Failed to deallocate resources for Session : %s" % str(session_id)
                LOG.warning(msg)
        LOG.info("[TERMINATE] Finish synthesis server connection")


    @staticmethod
    def process_command_line(argv):
        global operation_mode
        VERSION = 'VM Stream Server: %s' % StreamSynthesisConst.VERSION
        parser = OptionParser(usage="usage: %prog " + " [option]",
                version=VERSION)
        settings, args = parser.parse_args(argv)
        return settings, args

    def check_basevm(self):
        basevm_list = self.dbconn.list_item(BaseVM)
        ret_list = list()
        LOG.info("-"*50)
        LOG.info("* Base VM Configuration")
        for index, item in enumerate(basevm_list):
            # check file location
            (base_diskmeta, base_mempath, base_memmeta) = \
                    Cloudlet_Const.get_basepath(item.disk_path)
            if not os.path.exists(item.disk_path):
                LOG.warning("disk image (%s) is not exist" % (item.disk_path))
                continue
            if not os.path.exists(base_mempath):
                LOG.warning("memory snapshot (%s) is not exist" % (base_mempath))
                continue

            # add to list
            ret_list.append(item)
            LOG.info(" %d : %s (Disk %d MB, Memory %d MB)" % \
                    (index, item.disk_path, os.path.getsize(item.disk_path)/1024/1024, \
                    os.path.getsize(base_mempath)/1024/1024))
        LOG.info("-"*50)

        if len(ret_list) == 0:
            LOG.error("[Error] NO valid Base VM")
            sys.exit(2)
        return ret_list


if __name__ == "__main__":
    server = StreamSynthesisServer(sys.argv[1:])
    try:
        server.serve_forever()
    except Exception as e:
        #sys.stderr.write(str(e))
        server.terminate()
        sys.exit(1)
    except KeyboardInterrupt as e:
        sys.stdout.write("Exit by user\n")
        server.terminate()
        sys.exit(1)
    else:
        server.terminate()
        sys.exit(0)


