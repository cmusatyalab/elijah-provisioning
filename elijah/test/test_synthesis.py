import unittest

import os
import sys
# for local debugging
if os.path.exists("../provisioning") is True:
    sys.path.insert(0, "../../")
import urllib2
import time
import libvirt
import shutil
import random
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning import compression
from elijah.provisioning.cloudletfs import CloudletFS
from elijah.provisioning.configuration import Const as Cloudlet_Const
from elijah.provisioning.package import PackagingUtil
from elijah.provisioning.configuration import Options



class Const(object):
    disk_image_path = "/home/krha/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.raw"
    #overlay_url = "http://127.0.0.1:8000/temp-overlay"
    overlay_url = "http://128.2.213.110/overlay/temp-overlay"
    handoff_url = None
    base_vm_cirros_url =\
        "https://storage.cmusatyalab.org/cloudlet-vm/cirros-0.3.4-x86_64-base.zip"


class VMUtility(object):

    @staticmethod
    def get_VM_status(machine):
        machine_id = machine.ID()
        conn = libvirt.open("qemu:///session")
        try:
            for each_id in conn.listDomainsID():
                if each_id == machine_id:
                    each_machine = conn.lookupByID(machine_id)
                    vm_state, reason = each_machine.state(0)
                    return vm_state
        except libvirt.libvirtError as e:
            pass
        return None

    @staticmethod
    def download_baseVM(url, download_file):
        req = urllib2.urlopen(url)
        CHUNK_SIZE = 1024*1024
        with open(download_file, 'wb') as fd:
            while True:
                chunk = req.read(CHUNK_SIZE)
                if not chunk:
                    break
                fd.write(chunk)

    @staticmethod
    def delete_basevm(base_path, base_hashvalue):
        if base_path is not None and base_hashvalue is not None:
            dbconn, matching_basevm = PackagingUtil._get_matching_basevm(
                disk_path=base_path, hash_value=base_hashvalue)
            if matching_basevm:
                dbconn.del_item(matching_basevm)
            if matching_basevm:
                base_dir = os.path.dirname(base_path)
                shutil.rmtree(base_dir)


class TestCreatingVMOverlay(unittest.TestCase):

    def setUp(self):
        super(TestCreatingVMOverlay, self).setUp()
        self.temp_dir = mkdtemp(prefix="cloudlet-test-vmoverlay-")
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(Const.base_vm_cirros_url))
        VMUtility.download_baseVM(Const.base_vm_cirros_url,
                                  self.base_vm_cirros_filepath)
        self.base_vm_path, self.base_hashvalue = PackagingUtil.import_basevm(
            self.base_vm_cirros_filepath)
        self.overlay_filepath = None

    def tearDown(self):
        super(TestCreatingVMOverlay, self).tearDown()
        VMUtility.delete_basevm(self.base_vm_path, self.base_hashvalue)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        if self.overlay_filepath:
            os.remove(self.overlay_filepath)

    def test_create_vm_overlay(self):
        dbconn, matching_basevm = PackagingUtil._get_matching_basevm(
            disk_path=self.base_vm_path)

        base_diskpath = matching_basevm.disk_path
        options = Options()
        options.TRIM_SUPPORT = True
        options.ZIP_CONTAINER = True
        options.FREE_SUPPORT = False
        options.DISK_ONLY = False
        try:
            vm_overlay = synthesis.VM_Overlay(base_diskpath, options)
            machine = vm_overlay.resume_basevm()
            VM_status = VMUtility.get_VM_status(machine)
            self.assertEqual(VM_status, libvirt.VIR_DOMAIN_RUNNING)

            #wait for VM running
            time.sleep(10)
            vm_overlay.create_overlay()
            self.overlay_filepath = vm_overlay.overlay_zipfile
            self.assertTrue(os.path.exists(self.overlay_filepath), True)
        except Exception as e:
            self.assertTrue(False, "cannot create VM overlay: %s" % str(e))
            pass


class TestBaseExport(unittest.TestCase):

    def setUp(self):
        super(TestBaseExport, self).setUp()
        # import base VM to export it
        self.temp_dir = mkdtemp(prefix="cloudlet-test-basevm-")
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(Const.base_vm_cirros_url))
        VMUtility.download_baseVM(Const.base_vm_cirros_url,
                                  self.base_vm_cirros_filepath)
        self.base_vm_path, self.base_hashvalue = PackagingUtil.import_basevm(
            self.base_vm_cirros_filepath)

        # path for exported base VM
        self.export_outpath= os.path.join(self.temp_dir, "exported-base.zip")

    def tearDown(self):
        super(TestBaseExport, self).tearDown()
        VMUtility.delete_basevm(self.base_vm_path, self.base_hashvalue)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_export_base(self):
        dbconn, matching_basevm = PackagingUtil._get_matching_basevm(
            disk_path=self.base_vm_path)
        self.assertTrue(matching_basevm is not None,
                        "Cannot find the requested base VM")
        try:
            ret_package = PackagingUtil.export_basevm(
                self.export_outpath,
                matching_basevm.disk_path,
                matching_basevm.hash_value)
        except Exception as e:
            self.assertTrue(False, "Failed to export base VM")
        self.assertTrue(os.path.join(self.export_outpath))


class TestBaseImport(unittest.TestCase):

    def setUp(self):
        super(TestBaseImport, self).setUp()
        self.base_vm_path = None
        self.base_hashvalue = None
        self.temp_dir = mkdtemp(prefix="cloudlet-test-basevm-")
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(Const.base_vm_cirros_url))
        VMUtility.download_baseVM(Const.base_vm_cirros_url, self.base_vm_cirros_filepath)

    def tearDown(self):
        super(TestBaseImport, self).tearDown()
        VMUtility.delete_basevm(self.base_vm_path, self.base_hashvalue)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_import_base(self):
        try:
            self.base_vm_path, self.base_hashvalue = PackagingUtil.import_basevm(
                self.base_vm_cirros_filepath)
            self.assertTrue(os.path.exists(self.base_vm_path))
            self.assertIsInstance(self.base_hashvalue, str)
        except Exception as e:
            self.assertTrue(False, str(e))


class TestSynthesis(unittest.TestCase):

    def setUp(self):
        super(TestSynthesis, self).setUp()
        self.temperature = "cool"
        self.const = Const
        #self.addCleanup(self.attach_log_file)

        # check given parameters
        self.assertTrue(os.path.exists(self.const.disk_image_path),
                        "invalid disk path: %s" % self.const.disk_image_path)
        try:
            urllib2.urlopen(self.const.overlay_url)
        except Exception as e:
            self.assertTrue(False,
                            "Inlid iverlay URL: %s"  % self.const.overlay_url)

    def test_synthesis_step_by_step(self):
        # decompress VM overlay
        overlay_filename = NamedTemporaryFile(prefix="cloudlet-test-overlay-file-")
        meta_info = compression.decomp_overlayzip(self.const.overlay_url,
                                                  overlay_filename.name)
        self.assertIsInstance(meta_info, dict)
        self.assertTrue(meta_info.has_key(Cloudlet_Const.META_OVERLAY_FILES))

        # recover launch VM
        launch_disk, launch_mem, fuse, delta_proc, fuse_thread = \
            synthesis.recover_launchVM(self.const.disk_image_path, meta_info, overlay_filename.name)
        self.assertTrue(os.path.exists(launch_disk))
        self.assertTrue(os.path.exists(launch_mem))
        self.assertIsInstance(fuse, CloudletFS)

        # resume launch VM
        synthesized_VM = synthesis.SynthesizedVM(
            launch_disk, launch_mem, fuse, disk_only=False
        )
        delta_proc.start()
        fuse_thread.start()
        delta_proc.join()
        fuse_thread.join()
        synthesized_VM.resume()

        # check VM's status
        VM_status = VMUtility.get_VM_status(synthesized_VM.machine)
        self.assertEqual(VM_status, libvirt.VIR_DOMAIN_RUNNING)

        # cleanup
        synthesized_VM.monitor.terminate()
        synthesized_VM.monitor.join()
        synthesized_VM.terminate()

    def test_synthesis(self):
        try:
            synthesis.synthesis(
                self.const.disk_image_path,
                self.const.overlay_url,
                disk_only=False,
                zip_container=True,
                handoff_url=self.const.handoff_url,
                is_profiling_test=True      # turn off vnc
            )
        except Exception as e:
            self.assertTrue(False, str(e))
