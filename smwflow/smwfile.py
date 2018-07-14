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

import os
import pwd
import grp

def setattributes(_, obj):
    if 'smwpath' not in obj or not os.path.exists(obj['smwpath']):
        raise ValueError('no valid smwpath for %s' % obj['name'])
    statobj = os.stat(obj['smwpath'])

    if 'owner' in obj:
        passwd_data = pwd.getpwnam(obj['owner'])
        uid = passwd_data[2]
        os.chown(obj['smwpath'], uid, statobj.st_gid)

    statobj = os.stat(obj['smwpath'])
    if 'group' in obj:
        group_data = grp.getgrnam(obj['group'])
        gid = group_data[2]
        os.chown(obj['smwpath'], statobj.st_uid, gid)

    if 'mode' in obj:
        mode = int(obj['mode'])
        os.chmod(obj['smwpath'], mode)

def verifyattributes(_, obj):
    if 'smwpath' not in obj or not os.path.exists(obj['smwpath']):
        raise ValueError('no valid smwpath for %s' % obj['name'])
    statobj = os.stat(obj['smwpath'])

    if 'owner' in obj:
        passwd_data = pwd.getpwnam(obj['owner'])
        uid = passwd_data[2]
        if statobj.st_uid != uid:
            return False

    if 'group' in obj:
        group_data = grp.getgrnam(obj['group'])
        gid = group_data[2]
        if statobj.st_gid != gid:
            return False

    if 'mode' in obj:
        mode = int(obj['mode'])
        if (statobj.st_mode & 07777) != mode:
            return False
    return True
