# smwflow Copyright (c) 2018, The Regents of the University of California,
# through Lawrence Berkeley National Laboratory (subject to receipt of any
# required approvals from the U.S. Dept. of Energy). All rights reserved.
#
# If you have questions about your rights to use or distribute this software,
# please contact Berkeley Lab's Intellectual Property Office at  IPO@lbl.gov.
#
# NOTICE.  This Software was developed under funding from the U.S. Department
# of Energy and the U.S. Government consequently retains certain rights. As
# such, the U.S. Government has been granted for itself and others acting on its
# behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software
# to reproduce, distribute copies to the public, prepare derivative works, and
# perform publicly and display publicly, and to permit other to do so.
#
# See the LICENSE file in the top-level of the smwflow source distribution.

"""smwflow implements a git-based workflow for systems management.

This module implements all the wrapper (linkage) functionality needed to adapt a
Cray System Management Workstation to the smwflow git management system. It is
developed and intended to work with CLE6.0UP01 or newer.
"""

import copy

DEFAULT_GIT_BASEPATH = '/var/opt/cray/disk/1/git'
CONFIG_PATH = './etc'

__all__ = ["config", "process"]

class SmwflowObject(object):
    def __init__(self, name=None, smw_rel_path=None, parent=None, **kwargs):
        self.__data__ = dict()
        self.__set_callback__ = dict()
        self.__get_callback__ = dict()
        if name:
            self['name'] = name
        if smw_rel_path:
            self['smw_rel_path'] = smw_rel_path
        for key in kwargs:
            self[key] = kwargs[key]
        if parent_obj:
            self._copyconstructor(parent)
        self._basic_verify()

    def name(self):
        return self['name']

    def get_smw_path(self):
        return self['smw_path']

    def get_git_path(self):
        return self['git_path']

    def get_git_data(self):
        return self['git_data']

    def get_swm_data(self):
        return self['smw_data']

    def _copyconstructor(self, parent_obj):
        if not isinstance(parent_obj, SmwflowObject):
            raise ValueError("Invalid parent smwflow object")

        for key in parent_obj.keys():
            self[key] = parent_obj[key]

    def _basic_verify(self):
        if 'name' not in self:
            raise ValueError('Invalid name for SmwflowObject')
        if 'smw_rel_path' not in self:
            raise ValueError('Invalid smw_rel_path for SmwflowObject')

    def __getitem__(self, key):
        if key in self.__get_callback__:
            return self.__get_callback__[key](key)
        return self.__data__[key]

    def keys(self):
        keys = set(self.__get_callback__.keys())
        keys |= set(self.__set_callback__.keys())
        keys |= set(self.__data__.keys())
        return list(keys)

    def __setitem__(self, key, value):
        if key in self.__set_callback__:
            self.__set_callback__[key](key, value)
        else:
            self.__data__[key] = value

    def __delitem__(self, key):
        if key in self.__data__:
            del self.__data__[key]
        if key in self.__set_callback__:
            del self.__set_callback__[key]
        if key in self.__get_callback__:
            del self.__get_callback__[key]

    def __contains__(self, key):
        if key in self.__data__:
            return True
        if key in self.__set_callback__:
            return True
        if key in self.__get_callback__:
            return True
        return False

    def __len__(self):
        return len(self.keys())
