import os
import sys
import subprocess
import errno
import smwflow.compare
import smwflow.manifest
import smwflow.search
import smwflow.variables
import codecs
from jinja2 import Template

MANAGED_HSS = [
    {   'repo': 'smwconf',
        'name': 'blade_json.sedc',
        'smwpath': '/opt/cray/hss/default/etc/blade_json.sedc',
        'fstype': 'file',
        'formattype': 'json',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    },
    {   'repo': 'smwconf',
        'name': 'cab_json.sedc',
        'smwpath': '/opt/cray/hss/default/etc/cab_json.sedc',
        'fstype': 'file',
        'formattype': 'json',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'bm.ini',
        'smwpath': '/opt/cray/hss/default/etc/bm.ini',
        'fstype': 'file',
        'formattype': 'keyspacevalue',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'sm.ini',
        'smwpath': '/opt/cray/hss/default/etc/sm.ini',
        'fstype': 'file',
        'formattype': 'keyvalue',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'bm.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtbounce.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtcli.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtcli.ini',
        'fstype': 'file',
        'formattype': 'keyvalue',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtdiscover.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtdiscover.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtnlrd.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtnlrd.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtpcimon.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtpcimon.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtpmd.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtpmd.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtpmd_plugins.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtpmd_plugins.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'smwconf',
        'name': 'xtpowerd.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtpowerd.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'secured',
        'name': 'xtremoted.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtremoted/xtremoted.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0600,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'secured',
        'name': 'xtremoted.key',
        'smwpath': '/opt/cray/hss/default/etc/xtremoted/xtremoted.key',
        'fstype': 'file',
        'formattype': 'raw',
        'mode': 0400,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'secured',
        'name': 'xtremoted_ssl_ca.crt',
        'smwpath': '/opt/cray/hss/default/etc/xtremoted/ssl_ca.crt',
        'fstype': 'file',
        'formattype': 'raw',
        'mode': 0444,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'secured',
        'name': 'xtremoted.crt',
        'smwpath': '/opt/cray/hss/default/etc/xtremoted/xtremoted.crt',
        'fstype': 'file',
        'formattype': 'raw',
        'mode': 0444,
        'owner': 'crayadm',
        'group': 'crayadm'
    }, {
        'repo': 'secured',
        'name': 'xtremoted_rules.ini',
        'smwpath': '/opt/cray/hss/default/etc/xtremoted/rules.ini',
        'fstype': 'file',
        'formattype': 'ini',
        'mode': 0644,
        'owner': 'crayadm',
        'group': 'crayadm'
    },
]

MANAGED_HSS_FILES = { x['name']: x for x in MANAGED_HSS }

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
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise e
        repo_hss = os.path.join(repo_path, 'hss', '%s_hss' % config.system)
        try:
            os.mkdir(repo_hss, 0755)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise e

        manifest = smwflow.manifest.Manifest(repo_hss)

        for item in MANAGED_HSS:
            if item['repo'] != repo:
                continue
            if os.path.exists(item['smwpath']):
                tgt_path = os.path.join(repo_hss, item['name'])
                command = ['cp', item['smwpath'], tgt_path]
                rc = subprocess.call(command)
                manifest[item['name']] = item

        deferred_actions.extend(manifest.save())
        deferred_actions.append("Add/Commit HSS items in %s" % repo_path)
    return deferred_actions


def verify_data(config):
    deferred_actions = []

    objs = smwflow.search.get_objects(config, 'hss', 'hss')
    hss_vars = smwflow.variables.read_vars(config, 'hss', 'vars')
    for key in config.global_vars:
        if key in hss_vars:
            continue
        hss_vars[key] = config.global_vars[key]

    for key in objs:
        obj = objs[key]
        if 'smwpath' not in obj:
            print 'Skipping git hss file %s since it does not have an smwpath in the manifest' % key
            continue

        if not os.path.exists(obj['smwpath']):
            print 'git hss file %s does not exist as %s on SMW' % (key, obj['smwpath'])
            continue
        git_data = None
        smw_data = None
        with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
            data = rfp.read()
            template = Template(data)
            git_data = template.render(hss_vars)
        with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
            smw_data = rfp.read()

        if not git_data or not smw_data:
            print 'Failed to read git or smw data for hss file %s (smw: %s)' % (key, obj['smwpath'])
            continue
        issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""

    return deferred_actions
