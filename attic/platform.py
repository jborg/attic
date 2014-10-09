import os

platform = os.uname()[0]

def generic_walk_path(top):
    return os.walk(top)

if platform == 'Linux':
    from attic.platform_linux import acl_get, acl_set, API_VERSION, listdir_by_type
    from os.path import isdir as path_isdir, join as path_join

    def walk_path(top):
        """Directory tree generator, compatible with os.walk.

        The os.walk() implementation that comes with (at least) Python 3.4 does
        not take advantage of the fact that readdir_r() on most filesystems is
        able to distinguish between directories, regular files, etc.  This
        method uses platform_linux.listdir_by_type(), which does take advantage
        of this.  That makes it faster.

        Note: unlike os.walk(), this method does not support keyword arguments
        topdown, onerror, followlinks.  It behaves as if os.walk() were called
        with those arguments not provided.
        """

        try:
            dirs, nondirs, unknowns = listdir_by_type(top)
        except OSError:
            # Mimic os.walk() behaviour, which swallows errors.  The idea is
            # that if we lack permissions for some subdirectory, we keep going
            # anyway.
            return

        for name in unknowns:
            # Yes, listdir_by_type() may return directory entries with unknown
            # type.  This can happen depending on what the filesystem is.
            if path_isdir(path_join(top, name)):
                dirs.append(name)
            else:
                nondirs.append(name)

        yield top, dirs, nondirs

        for name in dirs:
            # TODO: this could be 'yield from' in Python >= 3.3
            for item in walk_path(path_join(top, name)):
                yield item

elif platform == 'FreeBSD':
    from attic.platform_freebsd import acl_get, acl_set, API_VERSION
    walk_path = generic_walk_path
    # TODO: supposedly, FreeBSD also supports the enhanced readdir_r() behaviour
    # that is mentioned in the docstring for the Linux version of walk_path().
elif platform == 'Darwin':
    from attic.platform_darwin import acl_get, acl_set, API_VERSION
    walk_path = generic_walk_path
else:
    API_VERSION = 1

    def acl_get(path, item, numeric_owner=False):
        pass
    def acl_set(path, item, numeric_owner=False):
        pass
