
import unittest
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

from elijah.test.util import Const as Const
from elijah.test.util import VMUtility
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning import compression
from elijah.provisioning.cloudletfs import CloudletFS
from elijah.provisioning.configuration import Const as Cloudlet_Const
from elijah.provisioning.package import PackagingUtil



class TestSynthesis(unittest.TestCase):

    def setUp(self):
        super(TestSynthesis, self).setUp()
        # check parameters
        self.overlay_url = Const.overlay_url_cirros
        try:
            urllib2.urlopen(self.overlay_url)
        except Exception as e:
            self.assertTrue(
                False,
                "Inlid overlay URL: %s\n%s" % (self.overlay_url, str(e))
            )

        # import base VM
        self.temp_dir = mkdtemp(prefix="cloudlet-test-vmoverlay-")
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(Const.base_vm_cirros_url))
        VMUtility.download_baseVM(Const.base_vm_cirros_url,
                                  self.base_vm_cirros_filepath)
        self.base_vm_path, self.base_hashvalue = PackagingUtil.import_basevm(
            self.base_vm_cirros_filepath)

    def tearDown(self):
        super(TestSynthesis, self).tearDown()
        VMUtility.delete_basevm(self.base_vm_path, self.base_hashvalue)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_synthesis_step_by_step(self):
        # decompress VM overlay
        overlay_filename = NamedTemporaryFile(prefix="cloudlet-test-overlay-file-")
        meta_info = compression.decomp_overlayzip(self.overlay_url,
                                                  overlay_filename.name)
        self.assertIsInstance(meta_info, dict)
        self.assertTrue(meta_info.has_key(Cloudlet_Const.META_OVERLAY_FILES))

        # recover launch VM
        launch_disk, launch_mem, fuse, delta_proc, fuse_thread = \
            synthesis.recover_launchVM(self.base_vm_path, meta_info, overlay_filename.name)
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
                self.base_vm_path,
                self.overlay_url,
                disk_only=False,
                zip_container=True,
                handoff_url=None,           # No handoff
                is_profiling_test=True      # turn off vnc
            )
        except Exception as e:
            self.assertTrue(False, str(e))
