import os
import subprocess
import io
import logging

platform = os.uname()[0]

if platform == 'Linux':
    from attic.platform_linux import acl_get, acl_set, API_VERSION
elif platform == 'FreeBSD':
    from attic.platform_freebsd import acl_get, acl_set, API_VERSION
elif platform == 'Darwin':
    from attic.platform_darwin import acl_get, acl_set, API_VERSION
elif platform.startswith('CYGWIN'):
    API_VERSION = 2

    def acl_get(path, item, st, numeric_owner=False):
        try:
            # Using non-numeric names, i.e., group names, has caused problems.
            # Hence the -n option
            acl_text = subprocess.check_output(['getfacl.exe', '-n', path])
            item[b'acl_access'] = acl_text
        except CalledProcessError as e:
            logging.warning('getfacl.exe failed with error code: ' + e.returncode)

    def acl_set(path, item, numeric_owner=False):
        try:
            acl_access = item[b'acl_access']
        except KeyError:
            return

        buf = io.StringIO(acl_access.decode('utf-8'))

        for line in buf:
            if len(line.strip()) > 0 and (not line.strip().startswith('#')):
                param = ['setfacl.exe', '-m', line.rstrip(), path]
                retVal = subprocess.call(param)
                if retVal != 0:
                    raise Exception('ACLs not set successfully')

else:
    API_VERSION = 1

    def acl_get(path, item, numeric_owner=False):
        pass
    def acl_set(path, item, numeric_owner=False):
        pass
