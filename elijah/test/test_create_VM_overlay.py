import unittest

import os
import sys
# for local debugging
if os.path.exists("../provisioning") is True:
    sys.path.insert(0, "../../")
import time
import libvirt
import shutil
from tempfile import mkdtemp
from elijah.test.util import Const as Const
from elijah.test.util import VMUtility
from elijah.provisioning import synthesis as synthesis
from elijah.provisioning.package import PackagingUtil
from elijah.provisioning.configuration import Options


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

            # wait for VM running
            time.sleep(10)
            vm_overlay.create_overlay()
            self.overlay_filepath = vm_overlay.overlay_zipfile
            self.assertTrue(os.path.exists(self.overlay_filepath), True)
        except Exception as e:
            self.assertTrue(False, "cannot create VM overlay: %s" % str(e))
