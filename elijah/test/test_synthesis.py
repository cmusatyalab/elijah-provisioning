from testtools import TestCase
from testtools.content import Content
from testtools.content_type import UTF8_TEXT
from testtools.matchers import Equals

import os
import sys
# for local debugging
if os.path.exists("../provisioning") is True:
    sys.path.insert(0, "../../")
import urllib2
import libvirt
import shutil
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning import compression
from elijah.provisioning.cloudletfs import CloudletFS
from elijah.provisioning.configuration import Const as Cloudlet_Const
from elijah.provisioning.package import PackagingUtil



class Const(object):
    disk_image_path = "/home/krha/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.raw"
    #overlay_url = "http://127.0.0.1:8000/temp-overlay"
    overlay_url = "http://128.2.213.110/overlay/temp-overlay"
    handoff_url = None


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


class TestBaseImport(TestCase):

    @staticmethod
    def _download_baseVM(url, download_file):
        req = urllib2.urlopen(url)
        CHUNK_SIZE = 1024*1024
        with open(download_file, 'wb') as fd:
            while True:
                chunk = req.read(CHUNK_SIZE)
                if not chunk:
                    break
                fd.write(chunk)

    @staticmethod
    def _delete_basevm(base_path, base_hashvalue):
        if base_path is not None and base_hashvalue is not None:
            dbconn, matching_basevm = PackagingUtil._get_matching_basevm(
                disk_path=base_path, hash_value=base_hashvalue)
            if matching_basevm:
                dbconn.del_item(matching_basevm)
            if matching_basevm:
                base_dir = os.path.dirname(base_path)
                shutil.rmtree(base_dir)


    def setUp(self):
        super(TestBaseImport, self).setUp()
        self.base_vm_path = None
        self.base_hashvalue = None
        self.temp_dir = mkdtemp(prefix="cloudlet-test-basevm-")
        self.base_vm_cirros_url =\
            "https://storage.cmusatyalab.org/cloudlet-vm/cirros-0.3.4-x86_64-base.zip"
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(self.base_vm_cirros_url))
        TestBaseImport._download_baseVM(self.base_vm_cirros_url,
                              self.base_vm_cirros_filepath)

    def tearDown(self):
        super(TestBaseImport, self).tearDown()
        TestBaseImport._delete_basevm(self.base_vm_path, self.base_hashvalue)
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


class TestSynthesis(TestCase):

    def setUp(self):
        super(TestSynthesis, self).setUp()
        self.temperature = "cool"
        self.const = Const()
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
