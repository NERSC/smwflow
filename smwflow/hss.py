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
import subprocess
import errno
import codecs
from jinja2 import Template
import smwflow
import smwflow.compare
import smwflow.manifest
import smwflow.search
import smwflow.smwfile
import smwflow.variables

MANAGED_HSS = [
    smwflow.SmwflowObject(
        repo='smwconf',
        name='blade_json.sedc',
        smwpath='/opt/cray/hss/default/etc/blade_json.sedc',
        fstype='file',
        formattype='json',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='cab_json.sedc',
        smwpath='/opt/cray/hss/default/etc/cab_json.sedc',
        fstype='file',
        formattype='json',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='bm.ini',
        smwpath='/opt/cray/hss/default/etc/bm.ini',
        fstype='file',
        formattype='keyspacevalue',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='sm.ini',
        smwpath='/opt/cray/hss/default/etc/sm.ini',
        fstype='file',
        formattype='keyvalue',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='bm.ini',
        smwpath='/opt/cray/hss/default/etc/xtbounce.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtcli.ini',
        smwpath='/opt/cray/hss/default/etc/xtcli.ini',
        fstype='file',
        formattype='keyvalue',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtdiscover.ini',
        smwpath='/opt/cray/hss/default/etc/xtdiscover.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtnlrd.ini',
        smwpath='/opt/cray/hss/default/etc/xtnlrd.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtpcimon.ini',
        smwpath='/opt/cray/hss/default/etc/xtpcimon.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtpmd.ini',
        smwpath='/opt/cray/hss/default/etc/xtpmd.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtpmd_plugins.ini',
        smwpath='/opt/cray/hss/default/etc/xtpmd_plugins.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='smwconf',
        name='xtpowerd.ini',
        smwpath='/opt/cray/hss/default/etc/xtpowerd.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='secured',
        name='xtremoted.ini',
        smwpath='/opt/cray/hss/default/etc/xtremoted/xtremoted.ini',
        fstype='file',
        formattype='ini',
        mode=0600,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='secured',
        name='xtremoted.key',
        smwpath='/opt/cray/hss/default/etc/xtremoted/xtremoted.key',
        fstype='file',
        formattype='raw',
        mode=0400,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='secured',
        name='xtremoted_ssl_ca.crt',
        smwpath='/opt/cray/hss/default/etc/xtremoted/ssl_ca.crt',
        fstype='file',
        formattype='raw',
        mode=0444,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='secured',
        name='xtremoted.crt',
        smwpath='/opt/cray/hss/default/etc/xtremoted/xtremoted.crt',
        fstype='file',
        formattype='raw',
        mode=0444,
        owner='crayadm',
        group='crayadm'
    ),
    smwflow.SmwflowObject(
        repo='secured',
        name='xtremoted_rules.ini',
        smwpath='/opt/cray/hss/default/etc/xtremoted/rules.ini',
        fstype='file',
        formattype='ini',
        mode=0644,
        owner='crayadm',
        group='crayadm'
    ),
]

MANAGED_HSS_FILES = {x['name']: x for x in MANAGED_HSS}

def import_data(config):
    deferred_actions = []
    for repo in ['smwconf', 'secured']:
        repo_path = getattr(config, repo, None)
        if not os.access(repo_path, os.W_OK):
            print "WARNING: cannot write to %s, skipping %s repo HSS item import" % (repo_path, repo)
            continue
        repo_hss = os.path.join(repo_path, 'hss')
        try:
            os.mkdir(repo_hss, 0755)
        except OSError, err:
            if err.errno != errno.EEXIST:
                raise err
        repo_hss = os.path.join(repo_path, 'hss', '%s_hss' % config.system)
        try:
            os.mkdir(repo_hss, 0755)
        except OSError, err:
            if err.errno != errno.EEXIST:
                raise err

        manifest = smwflow.manifest.Manifest(repo_hss)

        for item in MANAGED_HSS:
            if item['repo'] != repo:
                continue
            if os.path.exists(item['smwpath']):
                tgt_path = os.path.join(repo_hss, item['name'])
                command = ['cp', item['smwpath'], tgt_path]
                retcode = subprocess.call(command)
                if retcode != 0:
                    raise OSError('Failed to copy %s to %s' % (item['smwpath'], tgt_path))
                manifest[item['name']] = item

        deferred_actions.extend(manifest.save())
        deferred_actions.append("Add/Commit HSS items in %s" % repo_path)
    return deferred_actions

def _git_hss_object(_, obj, hss_vars):
    git_data = None
    with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
        data = rfp.read()
        template = Template(data)
        git_data = template.render(hss_vars)
    return git_data

def _smw_hss_object(_, obj):
    if not os.path.exists(obj['smwpath']):
        return None

    smw_data = None
    with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
        smw_data = rfp.read()
    return smw_data

def _valid_hss_object(_, obj, name):
    if 'smwpath' not in obj:
        print 'Skipping git hss file %s since it does not have an smwpath in the manifest' % name
        return False
    return True

def _verify_hss_object(config, obj, name, git_data, smw_data):
    if not git_data or not smw_data:
        print 'Failed to read git or smw data for hss file %s (smw: %s)' % (name, obj['smwpath'])
        return None
    issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
    return issues

def verify_data(config):
    deferred_actions = []

    objs = smwflow.search.get_objects(config, 'hss', 'hss')
    hss_vars = smwflow.variables.read_vars(config, 'hss', 'vars', None, config.global_vars)
    for key in objs:
        obj = objs[key]
        if not _valid_hss_object(config, obj, key):
            continue

        git_data = _git_hss_object(config, obj, hss_vars)
        smw_data = _smw_hss_object(config, obj)

        issues = _verify_hss_object(config, obj, key, git_data, smw_data)
        if isinstance(issues, list) and issues:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""
        if not smwflow.smwfile.verifyattributes(config, obj):
            print 'WARNING: file on smw %s has incorrect ownership or mode' % obj['smwpath']

    return deferred_actions

def update_data(config):
    deferred_actions = []
    objs = smwflow.search.get_objects(config, 'hss', 'hss')
    hss_vars = smwflow.variables.read_vars(config, 'hss', 'hss', None, config.global_vars)

    for key in objs:
        obj = objs[key]
        if not _valid_hss_object(config, obj, key):
            continue
        git_data = _git_hss_object(config, obj, hss_vars)
        smw_data = _smw_hss_object(config, obj)

        issues = _verify_hss_object(config, obj, key, git_data, smw_data)
        if issues is None or issues:
            print "Updating HSS component %s in %s" % (obj['name'], obj['smwpath'])
            with codecs.open(obj['smwpath'], mode='w', encoding='utf-8') as wfp:
                wfp.write(git_data)
            smwflow.smwfile.setattributes(config, obj)
    return deferred_actions
