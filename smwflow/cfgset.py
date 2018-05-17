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
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise e
        for objtype in ['worksheets','ansible','config', 'dist', 'files']:
            repo_imps = os.path.join(repo_path, 'imps', '%s_worksheets' % config.system)
            try:
                os.mkdir(repo_imps, 0755)
            except OSError, e:
                if e.errno != errno.EEXIST:
                     raise e

        manifest = smwflow.manifest.Manifest(repo_imps)

        for item in MANAGED_IMPS:
            if item['repo'] != repo:
                continue
            if os.path.exists(item['smwpath']):
                tgt_path = os.path.join(repo_imps, item['name'])
                command = ['cp', item['smwpath'], tgt_path]
                rc = subprocess.call(command)
                manifest[item['name']] = item

        deferred_actions.extend(manifest.save())
        deferred_actions.append("Add/Commit IMPS items in %s" % repo_path)
    return deferred_actions

def __get_smw_worksheets():
    pass


def verify_data(config):
    deferred_actions = []

    ctype = config.cfgset_type
    cname = config.cfgset_name

    objs = smwflow.search.get_objects(config, 'imps', 'worksheets', ctype)
    print objs
    return []
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars')
    for key in config.global_vars:
        if key in imps_vars:
            continue
        imps_vars[key] = config.global_vars[key]

    for key in objs:
        obj = objs[key]
        if 'smwpath' not in obj:
            print 'Skipping git imps file %s since it does not have an smwpath in the manifest' % key
            continue

        if not os.path.exists(obj['smwpath']):
            print 'git imps file %s does not exist as %s on SMW' % (key, obj['smwpath'])
            continue
        git_data = None
        smw_data = None
        with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
            data = rfp.read()
            template = Template(data)
            git_data = template.render(imps_vars)
        with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
            smw_data = rfp.read()

        if not git_data or not smw_data:
            print 'Failed to read git or smw data for imps file %s (smw: %s)' % (key, obj['smwpath'])
            continue
        issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""

    return deferred_actions
