import os
import sys
import subprocess
import pwd
import grp
import stat

def setattributes(config, obj):
    if 'smwpath' not in obj or not os.path.exists(obj['smwpath']):
        raise ValueError('no valid smwpath for %s' % obj['name'])
    st = os.stat(obj['smwpath'])

    if 'owner' in obj:
        passwd_data = pwd.getpwnam(obj['owner'])
        uid = passwd_data[2]
        os.chown(obj['smwpath'], uid, st.st_gid)

    st = os.stat(obj['smwpath'])
    if 'group' in obj:
        group_data = grp.getgrnam(obj['group'])
        gid = group_data[2]
        os.chown(obj['smwpath'], st.st_uid, gid)

    if 'mode' in obj:
        mode = int(obj['mode'])
        os.chmod(obj['smwpath'], mode)

def verifyattributes(config, obj):
    if 'smwpath' not in obj or not os.path.exists(obj['smwpath']):
        raise ValueError('no valid smwpath for %s' % obj['name'])
    st = os.stat(obj['smwpath'])

    if 'owner' in obj:
        passwd_data = pwd.getpwnam(obj['owner'])
        uid = passwd_data[2]
        if st.st_uid != uid:
            return False

    if 'group' in obj:
        group_data = grp.getgrnam(obj['group'])
        gid = group_data[2]
        if st.st_gid != gid:
            return False

    if 'mode' in obj:
        mode = int(obj['mode'])
        if (st.st_mode & 07777) != mode:
            return False
    return True
