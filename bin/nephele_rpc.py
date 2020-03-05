#!/usr/bin/env python
#
#   nephele
#
#   Author: Thomas Eiszler <teiszler@andrew.cmu.edu>
#
#   Copyright (C) 2019-2020 Carnegie Mellon University
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

import argparse
import datetime
import errno
import json
import glob
import libvirt
from libvirt import libvirtError
from lxml import etree
import msgpack
import os
import random
import shutil
import signal
import subprocess
import sys
from tempfile import NamedTemporaryFile
import textwrap
import threading
import time
from uuid import uuid4
import zipfile
from fabric.api import run, env
import socket
from argparse import Namespace #needed to parse args parameters that are sent via RPC
import rpyc

from elijah.provisioning import synthesis as synthesis
from elijah.provisioning import compression as compression
from elijah.provisioning.configuration import Const as Const
from elijah.provisioning.configuration import Options
import logging
import logging.config
import elijah.provisioning.db.table_def as table_def
from elijah.provisioning.db.api import DBConnector, log_op, update_op
from elijah.provisioning.package import PackagingUtil

DIR_NEPHELE = '/var/nephele/'
DIR_NEPHELE_IMAGES = '/var/nephele/images/'
DIR_NEPHELE_SNAPSHOTS = '/var/nephele/snapshots/'
DIR_NEPHELE_PID = '/var/nephele/pid/'
RPC_PORT = 19999

with open('/var/nephele/logging.json') as f:
    CONFIG_DICT = json.load(f)
    logging.config.dictConfig(CONFIG_DICT)

LOG = logging.getLogger(__name__)

class AuthenticationError(Exception):
    def __init__(self, peer):
        self.msg = 'Unauthorized connection.'
        LOG.error("Unauthorized connection attempt from %s", peer)

def SSHAuthenticator(sock):
    sockname = sock.getsockname()
    peername = sock.getpeername()
    if sockname[0] != '127.0.0.1':
        raise AuthenticationError(peername)
    if peername[0] != '127.0.0.1':
        raise AuthenticationError(peername)
    return sock, sock.getsockname()

class Nephele(rpyc.Service):
    def x_stdout_redirect(self, stdout):
        sys.stdout = stdout

    def x_stderr_redirect(self, stderr):
        sys.stderr = stderr

    def x_stdin_redirect(self, stdin):
        sys.stdin = stdin

    def x_restore(self):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        sys.stdin = sys.__stdin__

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def find_base(self, baseid):
        base = None
        dbconn = DBConnector()
        base_list = dbconn.list_item(table_def.BaseVM)
        for item in base_list:
            if baseid == item.hash_value[:12]:
                base = item
                break
        return dbconn, base

    def x_build_base(self, args):
        source = args.path
        disk_image_path = DIR_NEPHELE_IMAGES + source[source.rindex('/'):]

        # check if this filename already exists and warn
        if os.path.exists(disk_image_path):
            if not raw_input("An image with this filename already exists.\nAre you sure you wish to overwrite the following base image: %s? (y/N): " % (disk_image_path)).lower().strip().startswith("y"):
                sys.exit(1)
            if not raw_input("This will render any snapshots based on this image unusable. Are you certain? (y/N): ").lower().strip().startswith("y"):
                sys.exit(1)
        op_id = log_op(op=Const.OP_BUILD_IMAGE,notes="Source: %s" % (source))
        print "Copying/converting source image into %s..." % DIR_NEPHELE_IMAGES
        os.system('qemu-img convert -O raw %s %s' % (source, disk_image_path))

        disk_path, mem_path = synthesis.create_baseVM(disk_image_path, source=source, title=args.title, cpus=args.cpu, mem=args.mem)
        print "Created Base VM from this source image: %s" % source
        print "Base Disk Image: %s" % disk_path
        print "Base Memory Snapshot: %s" % mem_path
        update_op(op_id, has_ended=True, notes="Source: %s, Path: %s" % (source, disk_image_path))

        #restart the stream-server to reload list of images
        os.system('service stream-server restart')

    def x_build_snapshot(self, args):
        try:
            options = Options()
            options.DISK_ONLY = args.disk_only
            options.TRIM_SUPPORT = not args.no_trim
            options.FREE_SUPPORT = args.free_mem
            options.ZIP_CONTAINER = args.zip

            _, base = self.find_base(args.id)
            if base is None:
                print "Failed to find matching image with id: %s" % args.id
                return
            vm_overlay = synthesis.VM_Overlay(base.disk_path, options, qemu_args=None)
            machine = vm_overlay.resume_basevm(args.title)
            print 'Launching VM...\nPause VM when finished modifying to begin hashing of disk/memory state.'  
            #wait until VM is paused
            while True:
                state, _ = machine.state()
                if state == libvirt.VIR_DOMAIN_PAUSED:
                    break
            print 'VM entered paused state. Generating snapshot of disk and memory...'
            op_id = log_op(op=Const.OP_BUILD_SNAPSHOT,notes="Image: %s, Dest: %s" % (args.id, args.dest))
            vm_overlay.create_overlay()

            # print output
            if args.zip is False:
                LOG.info("overlay metafile (%ld) : %s",os.path.getsize(vm_overlay.overlay_metafile),
                                    vm_overlay.overlay_metafile)
                for overlay_file in vm_overlay.overlay_files:
                    LOG.info("overlay (%ld) : %s",os.path.getsize(overlay_file), overlay_file)
            else:
                overlay_zip = zipfile.ZipFile(vm_overlay.overlay_zipfile)
                filesize_count = 0
                for zipinfo in overlay_zip.infolist():
                    if zipinfo.filename == Const.OVERLAY_META:
                        msg = "meta file : (%ld) bytes" % (zipinfo.file_size)
                    else:
                        msg = "blob file : (%ld) bytes (%s)" % (zipinfo.file_size, zipinfo.filename)
                    filesize_count += zipinfo.file_size
                    LOG.info(msg)
                LOG.info("zip overhead : (%ld) bytes",os.path.getsize(vm_overlay.overlay_zipfile) - filesize_count)
        except Exception as e:
            print "Failed to create overlay: %s" % str(e)
        os.rename(vm_overlay.overlay_zipfile, args.dest)
        update_op(op_id, has_ended=True)
        # save the result to DB
        dbconn = DBConnector()
        snapshots = dbconn.list_item(table_def.Snapshot)
        for item in snapshots:
            if args.dest == item.path:
                dbconn.del_item(item)
                break
        new = table_def.Snapshot(args.dest, base.hash_value)
        dbconn.add_item(new)

    def x_list_base(self, args):
        dbconn = DBConnector()
        items = dbconn.list_item(table_def.BaseVM)
        output =  "{:<12}{:^8}{:<43}{:^8}{:<43}\n".format('IMAGE ID', '', 'NAME (/var/nephele/images/)', '', 'SOURCE')
        for item in items:
            output = output + "{:<12}{:^8}{:<43}{:^8}{:<43}\n".format(item.hash_value[:12],'', item.disk_path[item.disk_path.rindex('/')+1:], '', item.source)
        return output

    def x_list_snapshots(self, args):
        dbconn = DBConnector()
        items = dbconn.list_item(table_def.Snapshot)
        output = "{:<43}{:^8}{:<12}{:^8}{:<43}\n".format('SNAPSHOT', '', 'IMAGE ID', '', 'CREATED')
        for item in items:
            output = output + "{:<43}{:^8}{:<12}{:^8}{:<43}\n".format(item.path, '', item.basevm[:12], '', str(item.create_time))
        return output

    def x_list_instances(self, args):
        output = "{:<6}{:^1}{:<20}{:^8}{:<36}{:^8}{:<26}{:^1}{:<50}\n".format('PID', '', 'TITLE', '', 'UUID', '', 'STARTED', '', 'HANDOFFURL')
        for item in self.walk_nephele_pids():
            output = output + "{:<6}{:^1}{:<20}{:^8}{:<36}{:^8}{:<26}{:^1}{:<50}\n".format(item['pid'], '', item['title'], '', item['uuid'], '', item['started'], '', item['url'])
        return output

    def x_show_logs(self,args):
        dbconn = DBConnector()
        if(args.all):
            items = dbconn.session.query(table_def.Operations).order_by(table_def.Operations.id.desc())
        else:
            items = dbconn.session.query(table_def.Operations).order_by(table_def.Operations.id.desc()).limit(10)
        output = "{:<16}{:^8}{:<18}{:^8}{:<18}{:^8}{:<30}\n".format('OPERATION', '','STARTED', '','FINISHED', '', 'DETAILS')
        for item in items:
            output = output + "{:<16}{:^8}{:<18}{:^8}{:<18}{:^8}{:<30}\n".format(item.op, '', str(item.start_time)[:-7], '', str(item.end_time)[:-7], '', item.notes)
        return output

    def x_delete_snapshot(self, args):
        dbconn = DBConnector()
        if args.path:
            path = os.path.abspath(args.path)
        items = dbconn.list_item(table_def.Snapshot)
        snapshot = None
        for item in items:
            if path == item.path:
                snapshot = item
                break
        if snapshot is not None:
            if raw_input("Are you sure you wish to delete the following snapshot: %s? (y/N): " % (args.path)).lower().strip().startswith("y"):
                op_id = log_op(op=Const.OP_DELETE_SNAPSHOT,notes="Snapshot: %s" % (args.path))
                dbconn.del_item(snapshot)
                print "Snapshot removed from database."
                os.remove(args.path)
                print "Snapshot deleted from disk."
                update_op(op_id, has_ended=True)
        else:
            print "Cannot find matching snapshot at path %s!\n" % (args.path)
            return 1

    def x_clear_instances(self, args):
        if args.force or raw_input("Are you sure you wish to clear data? (y/N): ").lower().strip().startswith("y"):
            dbconn = DBConnector()
            if args.instances:
                list = dbconn.list_item(table_def.Instances)
                for item in list:
                    dbconn.del_item(item)
                print "Cleared instances table."
            if args.operations:
                list = dbconn.list_item(table_def.Operations)
                for item in list:
                    dbconn.del_item(item)
                print "Cleared operations table."
        return 0

    def x_delete_base(self, args):
        dbconn, base = self.find_base(args.id)
        if base is None:
            print "Failed to find matching image with id: %s!" % args.id
            return
        else:
            if raw_input("Are you sure you wish to delete the following base image: %s? (y/N): " % (base.disk_path)).lower().strip().startswith("y"):
                if raw_input("This will render any snapshots based on this image unusable. Are you certain? (y/N): ").lower().strip().startswith("y"):
                    op_id = log_op(op=Const.OP_DELETE_IMAGE,notes="Image: %s, Path: %s" % (args.id, base.disk_path))
                    dbconn.del_item(base)
                    print "Image removed from database."
                    ext = str(base.disk_path).rindex('.')
                    for f in glob.glob(base.disk_path[:ext] + '*'):
                        os.remove(f)
                    print "Image deleted from disk."
                    update_op(op_id, has_ended=True)

    def x_export_base(self, args):
        _, base = self.find_base(args.id)

        output_path = args.dest
        if base is not None:
            op_id = log_op(op=Const.OP_EXPORT_IMAGE, notes="Image: %s, Dest: %s" % (args.id, args.dest))
            PackagingUtil.export_basevm(
                output_path,
                base.disk_path,
                base.hash_value)
            update_op(op_id, has_ended=True)
        else:
            print "Failed to find matching image with id: %s" % args.id

    def x_import_base(self, args):
        source = args.path
        if os.path.exists(source) is False or os.access(source, os.R_OK) is False:
            print "Cannot read file: %s" % source
            return 1
    
        (base_hashvalue, disk_name, _, _, _) = \
                PackagingUtil._get_basevm_attribute(source)
        disk_image_path = DIR_NEPHELE_IMAGES + disk_name

        # check if this filename already exists and warn
        if os.path.exists(disk_image_path):
            if not raw_input("An image with this filename already exists.\nAre you sure you wish to overwrite the following base image: %s? (y/N): " % (disk_image_path)).lower().strip().startswith("y"):
                sys.exit(1)
            if not raw_input("This will render any snapshots based on this image unusable. Are you certain? (y/N): ").lower().strip().startswith("y"):
                sys.exit(1)
        op_id = log_op(op=Const.OP_IMPORT_IMAGE,notes="Image Path: %s" % (args.path))
        print "Decompressing image to %s..." % DIR_NEPHELE_IMAGES
        zipbase = zipfile.ZipFile(source, 'r')
        zipbase.extractall(DIR_NEPHELE_IMAGES)
        print "Extracted image files to %s." % (disk_image_path)

        # add to DB
        new_basevm = table_def.BaseVM(disk_image_path, base_hashvalue, source)
        dbconn = DBConnector()
        dbconn.add_item(new_basevm)
        update_op(op_id, has_ended=True)

        #restart the stream-server to reload list of images
        os.system('service stream-server restart')

    def x_import_snapshot(self, args):
        if not os.path.exists(args.path):
            msg = "Snapshot path (%s) does not exist!" % (args.path)
        else:
            url = 'file://' + os.path.abspath(args.path)
            overlay_filename = NamedTemporaryFile(prefix="cloudlet-overlay-file-")
            meta_info = compression.decomp_overlayzip(url, overlay_filename.name)

            base_sha = meta_info[Const.META_BASE_VM_SHA256]
            base_found = False
            dbconn = DBConnector()
            basevm_list = dbconn.list_item(table_def.BaseVM)
            for basevm_row in basevm_list:
                if basevm_row.hash_value == base_sha:
                    base_found = True
                    break
            if not base_found:
                msg = "Cannot find base image (SHA-256: %s) referenced in overlay: %s" % (base_sha, args.path)
            else:
                # save the result to DB
                items = dbconn.list_item(table_def.Snapshot)
                for item in items:
                    if args.path == item.path:
                        dbconn.del_item(item)
                        break
                new = table_def.Snapshot(args.path, base_sha)
                dbconn.add_item(new)
                msg = "Successfully imported snapshot (%s)." % (args.path)

        return msg

    def x_synthesize(self, args):
        err = False
        overlay_meta = args.snapshot
        is_zip_contained, url_path = PackagingUtil.is_zip_contained(
            overlay_meta)
        if is_zip_contained is True:
            overlay_meta = url_path
        LOG.info( "Beginning synthesis of: %s", args.snapshot)
        try:
            # generate pid file
            path = DIR_NEPHELE_PID + '%s.pid' % os.getpid()
            fdest = open(path, "wb")
            meta = dict()
            meta['title'] = args.title
            meta['pid'] = os.getpid()
            meta['started'] = str(datetime.datetime.now())
            meta['uuid'] = None
            meta['url'] = None
            fdest.write(msgpack.packb(meta))
            fdest.close()

            synthesis.synthesize(None, overlay_meta,
                                disk_only=args.disk_only,
                                handoff_url=None,
                                zip_container=is_zip_contained,
                                title=args.title,
                                fwd_ports=args.ports)

            if os.path.exists(path):
                os.unlink(path)
        except Exception as e:
            LOG.error("Failed to synthesize: %s", str(e))
            err = True

        if err:
            raise Exception(e)

    def x_snapshot_details(self, args):
        try:
            output = "\n" + synthesis.info_vm_overlay(args.path)
        except Exception as e:
            output = str(e)
        return output

    def walk_nephele_pids(self):
        instances = list()
        for root, dirs, files in os.walk(DIR_NEPHELE_PID):
            for filename in files:
                path = os.path.join(root, filename)
                with open(path, 'rb') as file:
                    meta = msgpack.unpackb(file.read())
                    instances.append(meta)
        return instances

    def x_handoff(self, args):
        handoff_url = 'tcp://' + args.dest + ':8022'

        #see if we can find a matching pid first by title, then uuid
        title_matched = False
        uuid_matched = False
        metadata = None
        matching_path = None
        for root, dirs, files in os.walk(DIR_NEPHELE_PID):
            for filename in files:
                path = os.path.join(root, filename)
                with open(path, 'rb') as file:
                    meta = msgpack.unpackb(file.read())
                    if meta["title"] == args.title:
                        if title_matched == True:
                            raise Exception("Ambiguous instance (TITLE: %s matches more than one instance) ; try using UUID instead!" % args.title)
                        title_matched = True
                        metadata = meta
                        matching_path = path
                    if meta["uuid"] == args.title:
                        uuid_matched = True
                        metadata = meta
                        matching_path = path

        if not uuid_matched and not title_matched:
            raise Exception("Could not find an instance (using TITLE or UUID) that matches %s!" % args.title)
        else:
            #send USR1 to that process and put the destination into the pid file
            fdest = open(matching_path, "wb")
            metadata['url'] = handoff_url
            fdest.write(msgpack.packb(metadata))
            fdest.close()
            os.kill(metadata['pid'], signal.SIGUSR1)


def _main():
    cfg = {"exposed_prefix":"x_"}
    t = rpyc.utils.server.ForkingServer(Nephele, logger=LOG, protocol_config=cfg, port=RPC_PORT, authenticator=SSHAuthenticator)
    t.start()

if __name__ == '__main__':
    _main()
