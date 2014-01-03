.. include:: global.rst.inc
.. _installation:

Installation
============

|project_name| requires Python_ 3.2 or above to work. Even though Python 3 is
not the default Python version on most Linux distributions it is usually
available as an optional install.

Other dependencies:

* `msgpack-python`_ >= 0.1.10
* OpenSSL_ >= 1.0.0
* Cython_ (only when installing straight from GIT)

The OpenSSL version bundled with Mac OS X and FreeBSD is most likey too old.
Newer versions are available from homebrew_ on OS X and from FreeBSD ports.

Installing from PyPI using pip
------------------------------
::

    $ pip install Attic

Installing from source tarballs
-------------------------------
.. parsed-literal::

    $ curl -O |package_url|
    $ tar -xvzf |package_filename|
    $ cd |package_dirname|
    $ python setup.py install

Installing from git
-------------------
.. parsed-literal::

    $ git clone |git_url|
    $ cd attic
    $ python setup.py install
