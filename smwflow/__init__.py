"""smwflow implements a git-based workflow for systems management.

This module implements all the wrapper (linkage) functionality needed to adapt a
Cray System Management Workstation to the smwflow git management system. It is
developed and intended to work with CLE6.0UP01 or newer.
"""

DEFAULT_GIT_BASEPATH = '/var/opt/cray/disk/1/git'
CONFIG_PATH = './etc'
__all__ = ["config", "process"]
