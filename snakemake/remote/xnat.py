__author__ = "Jon Stutters"
__copyright__ = "Copyright 2020, Jon Stutters"
__email__ = "jstutters@jeremah.co.uk"
__license__ = "MIT"

import os
import sys
import email.utils
from contextlib import contextmanager
import functools

# module-specific
from snakemake.remote import AbstractRemoteProvider, AbstractRemoteObject, DomainObject
from snakemake.exceptions import WebDAVFileException, WorkflowError
from snakemake.utils import os_sync

try:
    # third-party modules
    import pyxnat
except ImportError as e:
    raise WorkflowError(
        "The Python package 'pyxnat' must be present to use xnat remote() file "
        "functionality. %s" % e.msg
    )


class RemoteProvider(AbstractRemoteProvider):
    def __init__(
        self, *args, keep_local=False, stay_on_remote=False, is_default=False, **kwargs
    ):
        # loop = asyncio.get_event_loop()
        super(RemoteProvider, self).__init__(
            *args,
            keep_local=keep_local,
            stay_on_remote=stay_on_remote,
            is_default=is_default,
            **kwargs
        )

    @property
    def default_protocol(self):
        """The protocol that is prepended to the path when no protocol is specified."""
        return "https://"

    @property
    def available_protocols(self):
        """List of valid protocols for this remote provider."""
        return ["http://", "https://"]


class RemoteObject(DomainObject):
    """This is a class to interact with an XNAT file store."""

    def __init__(self, *args, keep_local=False, **kwargs):
        super(RemoteObject, self).__init__(*args, keep_local=keep_local, **kwargs)

    # === Implementations of abstract class members ===

    def exists(self):
        pass

    def mtime(self):
        if self.exists():
            pass
        else:
            raise WorkflowError(
                "The file does not seem to exist remotely: %s" % self.webdav_file
            )

    def size(self):
        if self.exists():
            pass

    def download(self, make_dest_dirs=True):
        if self.exists():
            # if the destination path does not exist, make it
            if make_dest_dirs:
                os.makedirs(os.path.dirname(self.local_file()), exist_ok=True)
            pass
        else:
            raise WorkflowError(
                "The file does not seem to exist remotely: %s" % self.webdav_file
            )

    def upload(self):
        pass

    @property
    def name(self):
        return self.local_file()

    @property
    def list(self):
        pass
