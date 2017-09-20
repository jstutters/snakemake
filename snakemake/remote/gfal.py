__author__ = "Johannes Köster"
__copyright__ = "Copyright 2017, Johannes Köster"
__email__ = "johannes.koester@tu-dortmund.de"
__license__ = "MIT"

import os
import re
import shutil
import subprocess as sp
from datetime import datetime
import time

from snakemake.remote import AbstractRemoteObject, AbstractRemoteProvider
from snakemake.exceptions import WorkflowError
from snakemake.common import lazy_property
from snakemake.logging import logger


if not shutil.which("gfal-copy"):
    raise WorkflowError("The gfal-* commands need to be available for "
                        "gfal remote support.")


class RemoteProvider(AbstractRemoteProvider):

    supports_default = True

    def __init__(self, *args, stay_on_remote=False, **kwargs):
        super(RemoteProvider, self).__init__(*args, stay_on_remote=stay_on_remote, **kwargs)
        self.retry = retry

    @property
    def default_protocol(self):
        """The protocol that is prepended to the path when no protocol is specified."""
        return "gsiftp://"

    @property
    def available_protocols(self):
        """List of valid protocols for this remote provider."""
        # TODO gfal provides more. Extend this list.
        return ["gsiftp://", "srm://"]


class RemoteObject(AbstractRemoteObject):
    mtime_re = re.compile("^\s+Modify: (.+)$", flags=re.MULTILINE)
    size_re = re.compile("^\s+Size: ([0-9]+).*$", flags=re.MULTILINE)

    def __init__(self, *args, keep_local=False, provider=None, **kwargs):
        super(RemoteObject, self).__init__(*args, keep_local=keep_local, provider=provider, **kwargs)

    def _gfal(self, cmd, *args):
        try:
            return sp.run(["gfal-" + cmd] + args,
                   check=True, stderr=sp.PIPE, stdout=sp.PIPE).stdout.decode()
        except sp.CalledProcessError as e:
            raise WorkflowError("Error calling gfal-{}:\n{}".format(
                cmd, e.stderr.decode()))

    # === Implementations of abstract class members ===

    def exists(self):
        files =self._gfal("ls", "-a", os.path.dirname(self.remote_file()))
        return os.path.basename(self.remote_file()) in files.splitlines()

    def _stat(self):
        stat = self._gfal("stat", self.remote_file())
        return stat

    def mtime(self):
        # assert self.exists()
        stat = self._stat()
        mtime = self.mtime_re.search(stat).group(1)
        date = datetime.strptime(mtime, "%Y-%m-%d %H:%M:%S.%f")
        return date.timestamp()

    def size(self):
        # assert self.exists()
        stat = self._stat()
        size = self.size_re.search(stat).group(1)
        return int(size)

    def download(self):
        if self.exists():
            os.makedirs(os.path.dirname(self.local_file()), exist_ok=True)

            # Download file. Wait for staging.
            source = self.remote_file()
            target = "file://" + os.path.abspath(self.local_file())

            self._gfal("copy", "-f", source, target)

            os.sync()
            return self.local_file()
        return None

    def upload(self):
        target = self.remote_file()
        parent = os.path.dirname(target)
        if parent:
            self._gfal("mkdir", "-p", parent)

        source = "file://" + os.path.abspath(self.local_file())
        self._gfal("copy", "-f", source, target)

    @property
    def list(self):
        # TODO implement listing of remote files with patterns
        raise NotImplementedError()

    def host(self):
        return self.local_file().split("/")[0]