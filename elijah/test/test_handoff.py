
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

    def test_handoff(self):
        handoff_file = os.path.abspath(os.path.join(self.temp_dir, "vm-residue.zip"))
        handoff_url = "file://%s" % (handoff_file)

        # 1. VM synthesis using VM overlay to create VM residue
        try:
            synthesis.synthesis(
                self.base_vm_path,
                self.overlay_url,
                disk_only=False,
                zip_container=True,
                handoff_url=handoff_url,
                is_profiling_test=True      # turn off vnc
            )
        except Exception as e:
            self.assertTrue(False, str(e))

        # 2. Check VM residue file
        self.assertTrue(os.path.exists(handoff_file))

        # 3. Resume VM with the VM residue file
        try:
            synthesis.synthesis(
                self.base_vm_path,
                handoff_url,
                disk_only=False,
                zip_container=True,
                handoff_url=None,
                is_profiling_test=True      # turn off vnc
            )
        except Exception as e:
            self.assertTrue(False, str(e))

