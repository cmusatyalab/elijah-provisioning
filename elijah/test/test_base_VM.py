import unittest
import os
import sys
# for local debugging
if os.path.exists("../provisioning") is True:
    sys.path.insert(0, "../../")
import shutil
from tempfile import mkdtemp

from elijah.test.util import Const as Const
from elijah.test.util import VMUtility
from elijah.provisioning.package import PackagingUtil


class TestBaseImport(unittest.TestCase):

    def setUp(self):
        super(TestBaseImport, self).setUp()
        self.base_vm_path = None
        self.base_hashvalue = None
        self.temp_dir = mkdtemp(prefix="cloudlet-test-basevm-")
        self.base_vm_cirros_filepath = os.path.join(
            self.temp_dir, os.path.basename(Const.base_vm_cirros_url))
        VMUtility.download_baseVM(Const.base_vm_cirros_url,
                                  self.base_vm_cirros_filepath)

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
        self.export_outpath = os.path.join(self.temp_dir, "exported-base.zip")

    def tearDown(self):
        super(TestBaseExport, self).tearDown()
        VMUtility.delete_basevm(self.base_vm_path, self.base_hashvalue)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_export_base(self):
        disk_path=self.base_vm_path
        dbconn, matching_basevm = PackagingUtil._get_matching_basevm(disk_path)
        self.assertTrue(matching_basevm is not None,
                        "Cannot find the requested base VM")
        try:
            PackagingUtil.export_basevm(
                self.export_outpath,
                matching_basevm.disk_path,
                matching_basevm.hash_value
            )
        except Exception as e:
            self.assertTrue(False, "Failed to export base VM: %s" % str(e))
        else:
            self.assertTrue(os.path.join(self.export_outpath))

