import os
import subprocess
import errno
import codecs
from jinja2 import Template
import smwflow.compare
import smwflow.manifest
import smwflow.search
import smwflow.smwfile
import smwflow.variables

MANAGED_IMPS = [
    {
        'repo': 'smwconf',
        'name': 'imps.json',
        'smwpath': '/etc/opt/cray/imps/imps.json',
        'fstype': 'file',
        'formattype': 'json',
        'mode': 0755,
        'owner': 'root',
        'group': 'root'
    },
    {
        'repo': 'smwconf',
        'name': 'image_recipes.local.json',
        'smwpath': '/etc/opt/cray/imps/image_recipes.d/image_recipes.local.json',
        'fstype': 'file',
        'formattype': 'json',
        'mode': 0644,
        'owner': 'root',
        'group': 'root'
    },
    {
        'repo': 'smwconf',
        'name': 'package_collections.local.json',
        'smwpath': '/etc/opt/cray/imps/package_collections.d/package_collections.local.json',
        'fstype': 'file',
        'formattype': 'json',
        'mode': 0644,
        'owner': 'root',
        'group': 'root'
    }
]

def import_data(config):
    deferred_actions = []
    for repo in ['smwconf', 'secured']:
        repo_path = getattr(config, repo, None)
        if not os.access(repo_path, os.W_OK):
            print "WARNING: cannot write to %s, skipping %s repo HSS item import" % (repo_path, repo)
            continue
        repo_imps = os.path.join(repo_path, 'imps')
        try:
            os.mkdir(repo_imps, 0755)
        except OSError, err:
            if err.errno != errno.EEXIST:
                raise err
        repo_imps = os.path.join(repo_path, 'imps', '%s_imps' % config.system)
        try:
            os.mkdir(repo_imps, 0755)
        except OSError, err:
            if err.errno != errno.EEXIST:
                raise err

        manifest = smwflow.manifest.Manifest(repo_imps)

        for item in MANAGED_IMPS:
            if item['repo'] != repo:
                continue
            if os.path.exists(item['smwpath']):
                tgt_path = os.path.join(repo_imps, item['name'])
                command = ['cp', item['smwpath'], tgt_path]
                retcode = subprocess.call(command)
                if retcode != 0:
                    raise OSError('Failed to copy %s to %s' % (item['smwpath'], tgt_path))
                manifest[item['name']] = item

        deferred_actions.extend(manifest.save())
        deferred_actions.append("Add/Commit IMPS items in %s" % repo_path)
    return deferred_actions

def _valid_imps_object(_, obj, name):
    if 'smwpath' not in obj:
        print 'Skipping git imps file %s since it does not have an smwpath in the manifest' % name
        return False
    return True

def _git_imps_object(_, obj, imps_vars):
    git_data = None
    with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
        data = rfp.read()
        template = Template(data)
        git_data = template.render(imps_vars)
    return git_data

def _smw_imps_object(_, obj, name):
    if not os.path.exists(obj['smwpath']):
        print 'git imps file %s does not exist as %s on SMW' % (name, obj['smwpath'])
        return None
    smw_data = None
    with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
        smw_data = rfp.read()
    return smw_data

def _verify_imps_object(config, obj, name, git_data, smw_data):
    if not git_data or not smw_data:
        print 'Failed to read git or smw data for imps file %s (smw: %s)' % (name, obj['smwpath'])
        return None
    issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
    return issues

def verify_data(config):
    deferred_actions = []

    objs = smwflow.search.get_objects(config, 'imps', 'imps')
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)

    for key in objs:
        obj = objs[key]
        if not _valid_imps_object(config, obj, key):
            continue

        git_data = _git_imps_object(config, obj, imps_vars)
        smw_data = _smw_imps_object(config, obj, key)

        issues = _verify_imps_object(config, obj, key, git_data, smw_data)
        if issues:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""
        if not smwflow.smwfile.verifyattributes(config, obj):
            print 'WARNING: file on smw %s has incorrect ownership or mode' % obj['smwpath']

    return deferred_actions

def update_data(config):
    deferred_actions = []
    objs = smwflow.search.get_objects(config, 'imps', 'imps')
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'imps', None, config.global_vars)

    for key in objs:
        obj = objs[key]
        if not _valid_imps_object(config, obj, key):
            continue
        git_data = _git_imps_object(config, obj, imps_vars)
        smw_data = _smw_imps_object(config, obj, key)

        issues = _verify_imps_object(config, obj, key, git_data, smw_data)
        if issues is None or issues:
            print "Updating IMPS component %s in %s" % (obj['name'], obj['smwpath'])
            with codecs.open(obj['smwpath'], mode='w', encoding='utf-8') as wfp:
                wfp.write(git_data)
        if not smwflow.smwfile.verifyattributes(config, obj):
            smwflow.smwfile.setattributes(config, obj)
    return deferred_actions
