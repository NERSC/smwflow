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
