import os, os.path
import shutil
import sys
import tempfile
import unittest
from attic.platform import acl_get, acl_set, walk_path
from attic.testsuite import AtticTestCase


ACCESS_ACL = """
user::rw-
user:root:rw-:0
user:9999:r--:9999
group::r--
group:root:r--:0
group:9999:r--:9999
mask::rw-
other::r--
""".strip().encode('ascii')

DEFAULT_ACL = """
user::rw-
user:root:r--:0
user:8888:r--:8888
group::r--
group:root:r--:0
group:8888:r--:8888
mask::rw-
other::r--
""".strip().encode('ascii')


def fakeroot_detected():
    return 'FAKEROOTKEY' in os.environ

class WalkPathTestCase(AtticTestCase):

    def touch(self, path):
        with open(path, 'w'):
            pass

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_walk_path(self):
        gen = walk_path(self.tmpdir)
        dirpath, dirnames, filenames = next(gen)
        self.assert_equal(dirpath, self.tmpdir)
        self.assert_equal(dirnames, [])
        self.assert_equal(filenames, [])
        with self.assert_raises(StopIteration):
            next(gen)

        self.touch(os.path.join(self.tmpdir, 'file'))
        os.makedirs(os.path.join(self.tmpdir, 'dir'))
        self.touch(os.path.join(self.tmpdir, 'dir', 'subfile'))
        self.touch(os.path.join(self.tmpdir, 'dir', 'subfile2'))

        gen = walk_path(self.tmpdir)
        dirpath, dirnames, filenames = next(gen)
        self.assert_equal(dirpath, self.tmpdir)
        self.assert_equal(dirnames, [ 'dir' ])
        self.assert_equal(filenames, [ 'file' ])
        dirpath, dirnames, filenames = next(gen)
        self.assert_equal(dirpath, os.path.join(self.tmpdir, 'dir'))
        self.assert_equal(dirnames, [])
        self.assert_equal(sorted(filenames), [ 'subfile', 'subfile2' ])
        with self.assert_raises(StopIteration):
            next(gen)


@unittest.skipUnless(sys.platform.startswith('linux'), 'linux only test')
@unittest.skipIf(fakeroot_detected(), 'not compatible with fakeroot')
class PlatformLinuxAclTestCase(AtticTestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def get_acl(self, path, numeric_owner=False):
        item = {}
        acl_get(path, item, os.stat(path), numeric_owner=numeric_owner)
        return item

    def set_acl(self, path, access=None, default=None, numeric_owner=False):
        item = {b'acl_access': access, b'acl_default': default}
        acl_set(path, item, numeric_owner=numeric_owner)

    def test_access_acl(self):
        file = tempfile.NamedTemporaryFile()
        self.assert_equal(self.get_acl(file.name), {})
        self.set_acl(file.name, access=b'user::rw-\ngroup::r--\nmask::rw-\nother::---\nuser:root:rw-:9999\ngroup:root:rw-:9999\n', numeric_owner=False)
        self.assert_in(b'user:root:rw-:0', self.get_acl(file.name)[b'acl_access'])
        self.assert_in(b'group:root:rw-:0', self.get_acl(file.name)[b'acl_access'])
        self.assert_in(b'user:0:rw-:0', self.get_acl(file.name, numeric_owner=True)[b'acl_access'])
        file2 = tempfile.NamedTemporaryFile()
        self.set_acl(file2.name, access=b'user::rw-\ngroup::r--\nmask::rw-\nother::---\nuser:root:rw-:9999\ngroup:root:rw-:9999\n', numeric_owner=True)
        self.assert_in(b'user:9999:rw-:9999', self.get_acl(file2.name)[b'acl_access'])
        self.assert_in(b'group:9999:rw-:9999', self.get_acl(file2.name)[b'acl_access'])

    def test_default_acl(self):
        self.assert_equal(self.get_acl(self.tmpdir), {})
        self.set_acl(self.tmpdir, access=ACCESS_ACL, default=DEFAULT_ACL)
        self.assert_equal(self.get_acl(self.tmpdir)[b'acl_access'], ACCESS_ACL)
        self.assert_equal(self.get_acl(self.tmpdir)[b'acl_default'], DEFAULT_ACL)


@unittest.skipUnless(sys.platform.startswith('darwin'), 'OS X only test')
@unittest.skipIf(fakeroot_detected(), 'not compatible with fakeroot')
class PlatformDarwinAclTestCase(AtticTestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def get_acl(self, path, numeric_owner=False):
        item = {}
        acl_get(path, item, os.stat(path), numeric_owner=numeric_owner)
        return item

    def set_acl(self, path, acl, numeric_owner=False):
        item = {b'acl_extended': acl}
        acl_set(path, item, numeric_owner=numeric_owner)

    def test_access_acl(self):
        file = tempfile.NamedTemporaryFile()
        file2 = tempfile.NamedTemporaryFile()
        self.assert_equal(self.get_acl(file.name), {})
        self.set_acl(file.name, b'!#acl 1\ngroup:ABCDEFAB-CDEF-ABCD-EFAB-CDEF00000000:staff:0:allow:read\nuser:FFFFEEEE-DDDD-CCCC-BBBB-AAAA00000000:root:0:allow:read\n', numeric_owner=False)
        self.assert_in(b'group:ABCDEFAB-CDEF-ABCD-EFAB-CDEF00000014:staff:20:allow:read', self.get_acl(file.name)[b'acl_extended'])
        self.assert_in(b'user:FFFFEEEE-DDDD-CCCC-BBBB-AAAA00000000:root:0:allow:read', self.get_acl(file.name)[b'acl_extended'])
        self.set_acl(file2.name, b'!#acl 1\ngroup:ABCDEFAB-CDEF-ABCD-EFAB-CDEF00000000:staff:0:allow:read\nuser:FFFFEEEE-DDDD-CCCC-BBBB-AAAA00000000:root:0:allow:read\n', numeric_owner=True)
        self.assert_in(b'group:ABCDEFAB-CDEF-ABCD-EFAB-CDEF00000000:wheel:0:allow:read', self.get_acl(file2.name)[b'acl_extended'])
        self.assert_in(b'group:ABCDEFAB-CDEF-ABCD-EFAB-CDEF00000000::0:allow:read', self.get_acl(file2.name, numeric_owner=True)[b'acl_extended'])

