#
# cloudlet-overlay.package
#
#   author: Kiryong Ha <krha@cmu.edu>
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

from cookielib import Cookie
from datetime import datetime
import dateutil.parser
from dateutil.tz import tzutc
import os
import re
import requests
import struct
import shutil
import urlparse
from lxml import etree
from tempfile import mkdtemp
from urlparse import urlsplit
from urllib import pathname2url
import zipfile
from lxml.builder import ElementMaker
import sys
import subprocess

from .configuration import Const
from . import log as logging
from .db.api import DBConnector
from .db.table_def import BaseVM

LOG = logging.getLogger(__name__)


class DetailException(Exception):

    def __init__(self, msg, detail=None):
        Exception.__init__(self, msg)
        if detail:
            self.detail = detail


class BadPackageError(DetailException):
    pass


class ImportBaseError(DetailException):
    pass


class NeedAuthentication(Exception):

    def __init__(self, host, realm, scheme):
        Exception.__init__(self, 'Authentication required')
        self.host = host
        self.realm = realm
        self.scheme = scheme


class _HttpError(Exception):

    '''_HttpFile would like to raise IOError on errors, but ZipFile swallows
    the error message.  So it raises this instead.'''
    pass


class _HttpFile(object):

    '''A read-only file-like object backed by HTTP Range requests.'''

    # pylint doesn't understand named tuples
    # pylint: disable=E1103

    def __init__(self, url, scheme=None, username=None, password=None,
                 buffer_size=64 << 10):
        if scheme == 'Basic':
            self._auth = (username, password)
        elif scheme == 'Digest':
            self._auth = requests.auth.HTTPDigestAuth(username, password)
        elif scheme is None:
            self._auth = None
        else:
            raise ValueError('Unknown authentication scheme')

        self.url = url
        self._offset = 0
        self._closed = False
        self._buffer = ''
        self._buffer_offset = 0
        self._buffer_size = buffer_size
        self._session = requests.Session()
        if hasattr(requests.utils, 'default_user_agent'):
            self._session.headers['User-Agent'] = 'cloudlet/%s %s' % (
                Const.VERSION, requests.utils.default_user_agent())
        else:
            # requests < 0.13.3
            self._session.headers['User-Agent'] = \
                'cloudleti-provisioning/%s python-requests/%s' % (
                    Const.VERSION, requests.__version__)

        # Debugging
        self._last_case = None
        self._last_network = None

        # Perform HEAD request
        try:
            resp = self._session.head(self.url, auth=self._auth)

            # Check for missing credentials
            if resp.status_code == 401:
                # Assumes a single challenge.
                scheme, parameters = resp.headers['WWW-Authenticate'].split(
                    None, 1)
                if scheme != 'Basic' and scheme != 'Digest':
                    raise _HttpError('Server requested unknown ' +
                                     'authentication scheme: %s' % scheme)
                host = urlsplit(self.url).netloc
                for param in parameters.split(', '):
                    match = re.match('^realm=\"([^"]*)\"$', param)
                    if match:
                        raise NeedAuthentication(host, match.group(1), scheme)
                raise _HttpError('Unknown authentication realm')

            # Check for other errors
            resp.raise_for_status()
            # 2xx codes other than 200 are unexpected
            if resp.status_code != 200:
                raise _HttpError('Unexpected status code %d' %
                                 resp.status_code)

            # Store object length
            try:
                self.length = int(resp.headers['Content-Length'])
            except (IndexError, ValueError):
                raise _HttpError('Server did not provide Content-Length')

            # Store validators
            self.etag = self._get_etag(resp)
            self.last_modified = self._get_last_modified(resp)

            # Record cookies
            if hasattr(self._session.cookies, 'extract_cookies'):
                # CookieJar
                self.cookies = tuple(c for c in self._session.cookies)
            else:
                # dict (requests < 0.12.0)
                parsed = urlsplit(self.url)
                self.cookies = tuple(
                    Cookie(version=0, name=name, value='"%s"' %
                           value, port=None, port_specified=False,
                           domain=parsed.netloc,
                           domain_specified=False,
                           domain_initial_dot=False,
                           path=parsed.path, path_specified=True,
                           secure=False, expires=None, discard=True,
                           comment=None, comment_url=None, rest={})
                    for name, value in self._session.cookies.iteritems())
        except requests.exceptions.RequestException as e:
            raise _HttpError(str(e))
    # pylint: enable=E1103

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()

    @property
    def name(self):
        return '<%s>' % self.url

    def _get_etag(self, resp):
        etag = resp.headers.get('ETag')
        if etag is None or etag.startswith('W/'):
            return None
        return etag

    def _get_last_modified(self, resp):
        last_modified = resp.headers.get('Last-Modified')
        if last_modified is None:
            return None
        try:
            return dateutil.parser.parse(last_modified)
        except ValueError:
            return None

    def _get(self, offset, size):
        range = '%d-%d' % (offset, offset + size - 1)
        self._last_network = range
        range = 'bytes=' + range

        try:
            resp = self._session.get(self.url, auth=self._auth, headers={
                'Range': range,
            })
            resp.raise_for_status()
            if resp.status_code != 206:
                raise _HttpError('Server ignored range request')
            if (self._get_etag(resp) != self.etag or
                    self._get_last_modified(resp) != self.last_modified):
                raise _HttpError('Resource changed on server')
            return resp.content
        except requests.exceptions.RequestException as e:
            raise _HttpError(str(e))

    def read(self, size=None):
        if self.closed:
            raise _HttpError('File is closed')
        if size is None:
            size = self.length - self._offset
        buf_start = self._buffer_offset
        buf_end = self._buffer_offset + len(self._buffer)
        if self._offset >= buf_start and self._offset + size <= buf_end:
            # Case B: Satisfy entirely from buffer
            self._last_case = 'B'
            start = self._offset - self._buffer_offset
            ret = self._buffer[start:start + size]
        elif self._offset >= buf_start and self._offset < buf_end:
            # Case C: Satisfy head from buffer
            # Buffer becomes _buffer_size bytes after requested region
            self._last_case = 'C'
            ret = self._buffer[self._offset - buf_start:]
            remaining = size - len(ret)
            data = self._get(self._offset + len(ret), remaining +
                             self._buffer_size)
            ret += data[:remaining]
            self._buffer = data[remaining:]
            self._buffer_offset = self._offset + size
        elif (self._offset < buf_start and
                self._offset + size >= buf_start):
            # Case D: Satisfy tail from buffer
            # Buffer becomes _buffer_size bytes before requested region
            # plus requested region
            self._last_case = 'D'
            tail = self._buffer[:self._offset + size - buf_start]
            start = max(self._offset - self._buffer_size, 0)
            data = self._get(start, buf_start - start)
            self._buffer = data + tail
            self._buffer_offset = start
            ret = self._buffer[self._offset - start:]
        else:
            # Buffer is useless
            if self._offset + size >= self.length:
                # Case E: Reading at the end of the file.
                # Assume zipfile is probing for the central directory.
                # Buffer becomes _buffer_size bytes before requested
                # region plus requested region
                self._last_case = 'E'
                start = max(self._offset - self._buffer_size, 0)
                self._buffer = self._get(start,
                                         self._offset + size - start)
                self._buffer_offset = start
                ret = self._buffer[self._offset - start:]
            else:
                # Case F: Read unrelated to previous reads.
                # Buffer becomes _buffer_size bytes after requested region
                self._last_case = 'F'
                data = self._get(self._offset, size + self._buffer_size)
                ret = data[:size]
                self._buffer = data[size:]
                self._buffer_offset = self._offset + size
        self._offset += len(ret)
        return ret

    def iter_content(self, offset, size, chunk_size):
        range = '%d-%d' % (offset, offset + size - 1)
        self._last_network = range
        range = 'bytes=' + range

        try:
            resp = self._session.get(self.url, auth=self._auth, headers={
                'Range': range,
            }, stream=True)
            for data in resp.iter_content(chunk_size):
                yield data
            '''
            resp.raise_for_status()
            if resp.status_code != 206:
                raise _HttpError('Server ignored range request')
            if (self._get_etag(resp) != self.etag or
                    self._get_last_modified(resp) != self.last_modified):
                raise _HttpError('Resource changed on server')
            return resp.content
            '''
        except requests.exceptions.RequestException as e:
            raise _HttpError(str(e))

    def seek(self, offset, whence=0):
        if self.closed:
            raise _HttpError('File is closed')
        if whence == 0:
            self._offset = offset
        elif whence == 1:
            self._offset += offset
        elif whence == 2:
            self._offset = self.length + offset
        self._offset = max(self._offset, 0)

    def tell(self):
        if self.closed:
            raise _HttpError('File is closed')
        return self._offset

    def close(self):
        self._closed = True
        self._buffer = ''
        self._session.close()

    @property
    def closed(self):
        return self._closed


class _FileFile(file):

    '''An _HttpFile-compatible file-like object for local files.'''

    # pylint doesn't understand named tuples
    # pylint: disable=E1103

    def __init__(self, url):
        # Process URL
        parsed = urlsplit(url)
        if parsed.scheme != 'file':
            raise ValueError('Invalid URL scheme')
        self.url = url
        self.cookies = ()

        file.__init__(self, parsed.path)

        # Set length
        self.seek(0, 2)
        self.length = self.tell()
        self.seek(0)

        # Set validators.
        self.etag = None
        self.last_modified = datetime.fromtimestamp(
            int(os.fstat(self.fileno()).st_mtime), tzutc())
    # pylint: enable=E1103

    def iter_content(self, offset, size, chunk_size):
        self.seek(offset)
        total_read = 0
        try:
            while total_read < size:
                data = self.read(chunk_size)
                yield data
        except:
            raise StopIteration()


class _PackageObject(object):

    def __init__(self, zip, path):
        self._fh = zip.fp
        self.url = self._fh.url
        self.etag = self._fh.etag
        self.last_modified = self._fh.last_modified
        self.cookies = self._fh.cookies

        # Calculate file offset and length
        try:
            info = zip.getinfo(path)
        except KeyError:
            raise BadPackageError('Path "%s" missing from package' % path)
        # ZipInfo.extra is the extra field from the central directory file
        # header, which may be different from the extra field in the local
        # file header.  So we need to read the local file header to determine
        # its size.
        header_fmt = '<4s5H3I2H'
        header_len = struct.calcsize(header_fmt)
        self._fh.seek(info.header_offset)
        magic, _, flags, compression, _, _, _, _, _, name_len, extra_len = \
            struct.unpack(header_fmt, self._fh.read(header_len))
        if magic != zipfile.stringFileHeader:
            raise BadPackageError('Member "%s" has invalid header' % path)
        if compression != zipfile.ZIP_STORED:
            raise BadPackageError('Member "%s" is compressed' % path)
        if flags & 0x1:
            raise BadPackageError('Member "%s" is encrypted' % path)
        self.offset = info.header_offset + header_len + name_len + extra_len
        self.size = info.file_size

    def iter_content(self, chunk_size):
        return self._fh.iter_content(self.offset, self.size, chunk_size)


class VMOverlayPackage(object):
    # pylint doesn't understand named tuples
    # pylint: disable=E1103

    def __init__(self, url, scheme=None, username=None, password=None):
        self.url = url

        # Open URL
        parsed = urlsplit(url)
        if parsed.scheme == 'http' or parsed.scheme == 'https':
            fh = _HttpFile(url, scheme=scheme, username=username,
                           password=password)
        elif parsed.scheme == 'file':
            fh = _FileFile(url)
        else:
            raise ValueError('%s: URLs not supported' % parsed.scheme)

        # Read Zip
        try:
            self.zip_overlay = zipfile.ZipFile(fh, 'r')

            if Const.OVERLAY_META not in self.zip_overlay.namelist():
                msg = "Does not have meta file named %s" % Const.OVERLAY_META
                raise BadPackageError(msg)

            self.metafile = Const.OVERLAY_META
            self.blobfiles = list()
            for each_file in self.zip_overlay.namelist():
                if (each_file != Const.OVERLAY_META):
                    self.blobfiles.append(each_file)

        except (zipfile.BadZipfile, _HttpError) as e:
            raise BadPackageError(str(e))
    # pylint: enable=E1103

    def read_meta(self):
        self.metadata = self.zip_overlay.read(self.metafile)
        return self.metadata

    def read_blob(self, blobname):
        return self.zip_overlay.read(blobname)

    def iter_blob(self, blobname, chunk_size):
        package_blob = _PackageObject(self.zip_overlay, blobname)
        return package_blob.iter_content(chunk_size)

    @classmethod
    def create(cls, outfilename, metafile, blobfiles):
        # Write package
        zip = zipfile.ZipFile(outfilename, 'w', zipfile.ZIP_STORED, True)
        zip.comment = 'Cloudlet VM overlay'
        zip.write(metafile, os.path.basename(metafile))
        for index, blobfile in enumerate(blobfiles):
            zip.write(blobfile, os.path.basename(blobfile))
        zip.close()


class BaseVMPackage(object):
    NS = 'http://opencloudlet.org/xmlns/vmsynthesis/package'
    NSP = '{' + NS + '}'
    SCHEMA_PATH = Const.BASEVM_PACKAGE_SCHEMA
    schema = etree.XMLSchema(etree.parse(SCHEMA_PATH))

    MANIFEST_FILENAME = 'basevm-package.xml'

    # pylint doesn't understand named tuples
    # pylint: disable=E1103
    def __init__(self, url, scheme=None, username=None, password=None):
        self.url = url

        # Open URL
        parsed = urlsplit(url)
        if parsed.scheme == 'http' or parsed.scheme == 'https':
            fh = _HttpFile(url, scheme=scheme, username=username,
                           password=password)
        elif parsed.scheme == 'file':
            fh = _FileFile(url)
        else:
            raise ValueError('%s: URLs not supported' % parsed.scheme)

        # Read Zip
        try:
            zip = zipfile.ZipFile(fh, 'r')

            # Parse manifest
            if self.MANIFEST_FILENAME not in zip.namelist():
                raise BadPackageError('Package does not contain manifest')
            xml = zip.read(self.MANIFEST_FILENAME)
            tree = etree.fromstring(xml, etree.XMLParser(schema=self.schema))

            # Create attributes
            self.base_hashvalue = tree.get('hash_value')
            self.disk = _PackageObject(
                zip, tree.find(self.NSP + 'disk').get('path'))
            self.memory = _PackageObject(
                zip, tree.find(self.NSP + 'memory').get('path'))
            self.disk_hash = _PackageObject(
                zip, tree.find(self.NSP + 'disk_hash').get('path'))
            self.memory_hash = _PackageObject(
                zip, tree.find(self.NSP + 'memory_hash').get('path'))
        except etree.XMLSyntaxError as e:
            raise BadPackageError('Manifest XML does not validate', str(e))
        except (zipfile.BadZipfile, _HttpError) as e:
            raise BadPackageError(str(e))
    # pylint: enable=E1103

    def read_meta(self):
        self.metadata = self.zip_overlay.read(self.metafile)
        return self.metadata

    @classmethod
    def create(cls, outfile, basevm_hashvalue,
               base_disk, base_memory, disk_hash, memory_hash):
        # Generate manifest XML
        e = ElementMaker(namespace=cls.NS, nsmap={None: cls.NS})
        tree = e.image(
            e.disk(path=os.path.basename(base_disk)),
            e.memory(path=os.path.basename(base_memory)),
            e.disk_hash(path=os.path.basename(disk_hash)),
            e.memory_hash(path=os.path.basename(memory_hash)),
            hash_value=str(basevm_hashvalue),
        )
        cls.schema.assertValid(tree)
        xml = etree.tostring(tree, encoding='UTF-8', pretty_print=True,
                             xml_declaration=True)
        zip = zipfile.ZipFile(outfile, 'w', zipfile.ZIP_DEFLATED, True)
        zip.comment = 'Cloudlet package for base VM'
        zip.writestr(cls.MANIFEST_FILENAME, xml)
        zip.close()

        # zip library bug at python 2.7.3
        # see more at http://bugs.python.org/issue9720

        #filelist = [base_disk, base_memory, disk_hash, memory_hash]
        # for filepath in filelist:
        #    basename = os.path.basename(filepath)
        #    filesize = os.path.getsize(filepath)
        #    LOG.info("Zipping %s (%ld bytes) into %s" % (basename, filesize, outfile))
        #    zip.write(filepath, basename)
        # zip.close()

        cmd = ['zip', '-j', '-9']
        cmd += ["%s" % outfile]
        cmd += [base_disk, base_memory, disk_hash, memory_hash]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, close_fds=True)
        LOG.info("Start compressing")
        LOG.info("%s" % ' '.join(cmd))
        for line in iter(proc.stdout.readline, ''):
            line = line.replace('\r', '')
            sys.stdout.write(line)
            sys.stdout.flush()


class PackagingUtil(object):

    @staticmethod
    def _get_matching_basevm(disk_path=None, hash_value=None):
        dbconn = DBConnector()
        if disk_path:
            disk_path = os.path.abspath(disk_path)
        basevm_list = dbconn.list_item(BaseVM)
        ret_basevm = None
        for item in basevm_list:
            if (disk_path is not None) and (disk_path == item.disk_path):
                ret_basevm = item
                break
            if (hash_value is not None) and (hash_value == item.hash_value):
                ret_basevm = item
                break
        return dbconn, ret_basevm

    @staticmethod
    def export_basevm(name, basevm_path, basevm_hashvalue):
        (base_diskmeta, base_mempath, base_memmeta) = \
            Const.get_basepath(basevm_path)
        output_path = os.path.join(os.curdir, name)
        if output_path.endswith(".zip") == False:
            output_path += ".zip"
        if os.path.exists(output_path):
            is_overwrite = raw_input(
                "%s exists. Overwirte it? (y/N) " %
                output_path)
            if is_overwrite != 'y':
                return None

        BaseVMPackage.create(
            output_path,
            basevm_hashvalue,
            basevm_path,
            base_mempath,
            base_diskmeta,
            base_memmeta)
        #BaseVMPackage.create(output_path, name, base_diskmeta, base_memmeta, base_diskmeta, base_memmeta)
        return output_path

    @staticmethod
    def _get_basevm_attribute(zipped_file):
        zipped_file = os.path.abspath(zipped_file)
        # Parse manifest
        zip = zipfile.ZipFile(_FileFile("file:///%s" % zipped_file), 'r')
        if BaseVMPackage.MANIFEST_FILENAME not in zip.namelist():
            raise BadPackageError('Package does not contain manifest')
        xml = zip.read(BaseVMPackage.MANIFEST_FILENAME)
        tree = etree.fromstring(
            xml, etree.XMLParser(schema=BaseVMPackage.schema)
        )

        # Create attributes
        base_hashvalue = tree.get('hash_value')
        disk_name = tree.find(BaseVMPackage.NSP + 'disk').get('path')
        memory_name = tree.find(BaseVMPackage.NSP + 'memory').get('path')
        diskhash_name = tree.find(BaseVMPackage.NSP + 'disk_hash').get('path')
        memoryhash_name = tree.find(
            BaseVMPackage.NSP + 'memory_hash').get('path')
        zip.close()

        return base_hashvalue, disk_name, memory_name, diskhash_name, memoryhash_name

    @staticmethod
    def import_basevm(filename):
        filename = os.path.abspath(filename)
        (base_hashvalue, disk_name, memory_name, diskhash_name, memoryhash_name) = \
            PackagingUtil._get_basevm_attribute(filename)

        # check duplica
        base_vm_dir = os.path.join(
            os.path.dirname(Const.BASE_VM_DIR), base_hashvalue)
        temp_dir = mkdtemp(prefix="cloudlet-base-")
        disk_tmp_path = os.path.join(temp_dir, disk_name)
        disk_target_path = os.path.join(base_vm_dir, disk_name)
        dbconn, matching_basevm = PackagingUtil._get_matching_basevm(
            disk_path=disk_target_path, hash_value=base_hashvalue)
        if matching_basevm is not None:
            msg = ("Base VM is already exists. "
                   "Delete existing Base VM using command. "
                   "See more 'cloudlet --help'")
            raise ImportBaseError(msg)
        if not os.path.exists(base_vm_dir):
            LOG.info("create directory for base VM")
            os.makedirs(base_vm_dir)

        # decompress
        LOG.info("Decompressing Base VM to temp directory at %s" % temp_dir)
        zipbase = zipfile.ZipFile(_FileFile("file:///%s" % filename), 'r')
        zipbase.extractall(temp_dir)
        shutil.move(disk_tmp_path, disk_target_path)
        (target_diskhash, target_memory, target_memoryhash) = \
            Const.get_basepath(disk_target_path, check_exist=False)
        path_list = {
            os.path.join(temp_dir, memory_name): target_memory,
            os.path.join(temp_dir, diskhash_name): target_diskhash,
            os.path.join(temp_dir, memoryhash_name): target_memoryhash,
            }

        LOG.info("Place base VM to a right directory")
        for (src, dest) in path_list.iteritems():
            shutil.move(src, dest)

        # add to DB
        LOG.info("Register New Base to DB")
        LOG.info("ID for the new Base VM: %s" % base_hashvalue)
        new_basevm = BaseVM(disk_target_path, base_hashvalue)
        LOG.info("Success")
        dbconn.add_item(new_basevm)
        return disk_target_path, base_hashvalue

    @staticmethod
    def is_zip_contained(filepath):
        if os.path.exists(filepath):
            # refular file
            abspath = os.path.abspath(filepath)
            urlpath = urlparse.urljoin("file:", pathname2url(abspath))
        else:
            urlpath = filepath
        try:
            overlay = VMOverlayPackage(urlpath)
            return True, urlpath
        except Exception as e:
            return False, None
