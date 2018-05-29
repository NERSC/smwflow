## Author: Douglas Jacobsen <dmjacobsen@lbl.gov>

import os
import sys
import stat
import subprocess
import errno
import smwflow.compare
import smwflow.manifest
import smwflow.search
import smwflow.smwfile
import smwflow.variables
import codecs
import rsm.hss
from jinja2 import Template


MANAGED_CFGSET_WORKSHEET = {
}

MANAGED_CFGSET_CONFIG = {
    'cray_lmt_config.yaml': { 'mode': 0600 },
    'cray_local_users_config.yaml': { 'mode': 0600 },
}

class cfgset(object):
    def __init__(self, config, ctype, cname):
        self.config = config
        self.cfgset_type = ctype
        self.cfgset_name = cname
        self.routermap = {}
        routermap = rsm.hss.RouterMap(self.config.partition)
        for node in routermap:
            self.routermap[node.cname] = node

    def _render_obj(self, obj, objtype_vars):
        git_data = None
        with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
            data = rfp.read()
            template = Template(data)
            git_data = template.render(objtype_vars)
        return git_data

    def _simple_worksheet_config(self, data):
        ret = {}
        for key in data:
            subkeys = key.split(".")
            obj = ret
            parentobj = None
            for skey in subkeys:
                if skey not in obj:
                    obj[skey] = {}
                parentobj = obj
                obj = obj[skey]
            parentobj[subkeys[-1]] = data[key]
        return ret

    def parse_network(self, system=None):
        if not system:
            system = config['system']

        objs = smwflow.search.get_objects(self.config, 'imps', 'worksheets', self.cfgset_type, system=system)
        global_vars = smwflow.variables.read_vars(self.values, 'vars', 'vars', system=system)
        imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, global_vars, system=system)
        worksheet_vars = smwflow.variables.read_vars(self.config, 'imps', 'worksheet_vars', ctype, imps_vars, system=system)

        net_worksheet = self._render_obj(objs['cray_net_worksheet.yaml'], worksheet_vars)
        data = yaml.load(net_worksheet)
        return _simple_worksheet_config(data)

    def parse_nodegroups(self, system=None):
        if not system:
            system = config['system']
        objs = smwflow.search.get_objects(self.config, 'imps', 'worksheets', self.cfgset_type, system=system)
        global_vars = smwflow.variables.read_vars(self.values, 'vars', 'vars', system=system)
        imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, global_vars, system=system)
        worksheet_vars = smwflow.variables.read_vars(self.config, 'imps', 'worksheet_vars', ctype, imps_vars, system=system)

        worksheet = self._render_obj(objs['cray_node_groups_worksheet.yaml'], worksheet_vars)
        data = yaml.load(worksheet)
        return _simple_worksheet_config(data)

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
            repo_imps = os.path.join(repo_path, 'imps', '%s_%s' % (config.system, objtype))
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

def _get_smw_dist_preload(config, ctype, cname):
    smw_files = {}
    dist_path = os.path.join(config.configset_path, cname, 'dist')
    if not os.path.exists(dist_path):
        raise ValueError('cfgset %s does not exist' % dist_path)
    paths = os.listdir(dist_path)
    for fname in paths:
        path = os.path.join(dist_path, fname)
        st = os.stat(path)
        if stat.S_ISREG(st.st_mode) and fname.find('preload') >= 0 and fname.find('cray') < 0:
            smw_files[fname] = path
    return smw_files

def _get_smw_worksheets(config, ctype, cname, extra):
    smw_worksheets = {}
    managed_smw_worksheets = {}
    cfgset_path = os.path.join(config.configset_path, cname)
    if not os.path.exists(cfgset_path): 
        raise ValueError('cfgset %s does not exist' % cfgset_path)
    worksheet_path = os.path.join(cfgset_path, 'worksheets')
    filenames = os.listdir(worksheet_path)
    for filename in filenames:
        path = os.path.join(worksheet_path, filename)
        st = os.stat(path)
        if stat.S_ISREG(st.st_mode) and filename.endswith('worksheet.yaml'):
            smw_worksheets[filename] = {'smwpath': path}
            if filename in extra:
                for key in extra[filename]:
                    if key not in smw_worksheets[filename]:
                        smw_worksheets[filename][key] = extra[filename][key]
    for filename in extra:
        obj = {'smwpath': os.path.join(worksheet_path, filename)}
        for key in extra[filename]:
            obj[key] = extra[filename][key]
        managed_smw_worksheets[filename] = obj
    return smw_worksheets,managed_smw_worksheets

def _get_smw_configs(config, ctype, cname, extra):
    smw_configs = {}
    managed_smw_configs = {}
    cfgset_path = os.path.join(config.configset_path, cname)
    if not os.path.exists(cfgset_path): 
        raise ValueError('cfgset %s does not exist' % cfgset_path)
    config_path = os.path.join(cfgset_path, 'config')
    filenames = os.listdir(config_path)
    for filename in filenames:
        path = os.path.join(config_path, filename)
        st = os.stat(path)
        if not stat.S_ISREG(st.st_mode):
            continue
        if filename != 'cray_image_groups.yaml' and filename.startswith('cray_'):
            continue
        if filename in extra:
            for key in extra[filename]:
                if key not in smw_worksheets[filename]:
                    smw_configs[filename][key] = extra[filename][key]
    for filename in extra:
        obj = {'smwpath': os.path.join(config_path, filename)}
        for key in extra[filename]:
            obj[key] = extra[filename][key]
        managed_smw_configs[filename] = obj
        
    return smw_configs, managed_smw_configs

def _render_obj(config, obj, objtype_vars):
    git_data = None
    with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
        data = rfp.read()
        template = Template(data)
        git_data = template.render(objtype_vars)
    return git_data

def _read_smw_obj(config, obj):
    smw_data = None
    if 'smwpath' not in obj or not obj['smwpath']:
        print 'unknown smw path for %s' % obj['name']
        return None
    if not os.path.exists(obj['smwpath']):
        print 'file does not exist for %s at %s' % (obj['name'], obj['smwpath'])
        return None

    with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
        smw_data = rfp.read()
    return smw_data

def _verify_worksheets(config, imps_vars, ctype, cname):
    deferred_actions = []
    worksheet_vars = smwflow.variables.read_vars(config, 'imps', 'worksheet_vars', ctype, imps_vars)

    objs = smwflow.search.get_objects(config, 'imps', 'worksheets', ctype, MANAGED_CFGSET_WORKSHEET)
    smw_worksheets, managed_smw_cfgset_worksheets = _get_smw_worksheets(config, ctype, cname, MANAGED_CFGSET_WORKSHEET)
    print smw_worksheets
    smw_keys = set(smw_worksheets.keys())
    git_keys = set(objs.keys())
    common = smw_keys.intersection(git_keys)
    smw_only = smw_keys - git_keys
    git_only = git_keys - smw_keys
    if len(smw_only) > 0:
        print 'worksheets on smw missing from git: ', smw_only
        print
    if len(git_only) > 0:
        print 'worksheets in git missing from smw: ', git_only
        print

    for key in common:
        obj = objs[key]
        obj['name'] = key
        obj['smwpath'] = smw_worksheets[key]['smwpath']

        git_data = _render_obj(config, obj, worksheet_vars)
        smw_data = _read_smw_obj(config, obj)

        if not git_data or not smw_data:
            print 'Failed to read git or smw data for imps file %s (smw: %s)' % (key, obj['smwpath'] if 'smwpath' in obj else 'Unknown')
            continue
        issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""
    for key in managed_smw_cfgset_worksheets:
        obj = maanged_smw_cfgset_worksheets[key]
        if not smwflow.smwfile.verifyattributes(config, obj):
            print "WARNING: %s has incorrect ownership or file mode" % (obj['smwpath'])

    return deferred_actions

def _verify_dist_preload(config, imps_vars, ctype, cname):
    deferred_actions = []
    dist_vars = smwflow.variables.read_vars(config, 'imps', 'dist_vars', ctype, imps_vars)
    objs = smwflow.search.get_objects(config, 'imps', 'dist', ctype)
    smw_dists = _get_smw_dist_preload(config, ctype, cname)
    smw_keys = set(smw_dists.keys())
    git_keys = set(objs.keys())
    common = smw_keys.intersection(git_keys)
    smw_only = smw_keys - git_keys
    git_only = git_keys - smw_keys

    if len(smw_only) > 0:
        print 'dist files on smw missing from git: ', smw_only
        print
    if len(git_only) > 0:
        print 'dist files in git missing from smw: ', git_only
        print
    for key in common:
        obj = objs[key]
        obj['name'] = key
        print obj
        smwpath = smw_configs[key]

        git_data = _render_obj(config, obj, config_vars)
        smw_data = _read_smw_obj(config, obj)

        if not git_data or not smw_data:
            print 'Failed to read git or smw data for dist file %s (smw: %s)' % (key, smwpath)
            continue
        issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""
        if not smwflow.smwfile.verifyattributes(config, obj):
            print "WARNING: %s has incorrect ownership or file mode" % (obj['smwpath'])
    return deferred_actions

def _verify_configs(config, imps_vars, ctype, cname):
    """
    This routine only verifies non-cray config files injected into the cfgset from git.
    Assumes anything starting with cray_ is managed by the worksheets
    """
    deferred_actions = []
    config_vars = smwflow.variables.read_vars(config, 'imps', 'config_vars', ctype, imps_vars)
    objs = smwflow.search.get_objects(config, 'imps', 'config', ctype, MANAGED_CFGSET_CONFIG)
    smw_configs, managed_smw_configs = _get_smw_configs(config, ctype, cname, MANAGED_CFGSET_CONFIG)
    smw_keys = set(smw_configs.keys())
    git_keys = set(objs.keys())
    common = smw_keys.intersection(git_keys)
    smw_only = smw_keys - git_keys
    git_only = git_keys - smw_keys
    if len(smw_only) > 0:
        print 'config files on smw missing from git: ', smw_only
        print
    if len(git_only) > 0:
        print 'config files in git missing from smw: ', git_only
        print
    for key in common:
        obj = objs[key]
        obj['name'] = key
        obj['smwpath'] = smw_configs[key]

        git_data = _render_obj(config, obj, config_vars)
        smw_data = _read_smw_obj(config, obj)

        if not git_data or not smw_data:
            print 'Failed to read git or smw data for worksheet config file %s (smw: %s)' % (key, smwpath)
            continue
        issues = smwflow.compare.basic_compare(config, obj, git_data, smw_data)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""
    for key in managed_smw_configs:
        obj = managed_smw_configs[key]
        if not smwflow.smwfile.verifyattributes(config, obj):
            print "WARNING: %s has incorrect ownership or file mode" % (obj['smwpath'])

    return deferred_actions

def _verify_ansible(config, imps_vars, ctype, cname):
    cfgset_ansible_path = os.path.join(config.configset_path, cname, 'ansible')
    cfgset_ansible_path = os.path.realpath(cfgset_ansible_path)

    # first, walk the cfgset and build a list of all existing paths
    start_idx = len(cfgset_ansible_path) + 1
    cfgset_ansible = {}
    for (dirpath, dirnames, filenames) in os.walk(cfgset_ansible_path):
        for path in filenames:
            relpath =  os.path.join(dirpath, path)[start_idx:]
            cfgset_ansible[relpath] = { 'match': False, 'checked': False, 'smwpath': os.path.join(dirpath, path) }

    # next, walk the repos and discover a list of objects
    objs = smwflow.search.get_objects(config, 'imps', 'ansible', ctype)

    # execute any discovered ansible smwflow plugins, generate content in-memory
    ## TODO

    smw_keys = set(cfgset_ansible.keys())
    git_keys = set(objs.keys())

    smwonly_keys = smw_keys - git_keys
    gitonly_keys = git_keys - smw_keys
    common_keys = smw_keys.intersection(git_keys)

    if len(smwonly_keys) > 0:
        print "ansible files only on smw: ", smwonly_keys
        print

    if len(gitonly_keys) > 0:
        print  "ansible files only in git: ", gitonly_keys
        print

    for key in common_keys:
        obj = objs[key]
        obj['smwpath'] = cfgset_ansible[key]['smwpath']
        smw_value = None
        git_value = None

        with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
            git_value = rfp.read()
        with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
            smw_value = rfp.read()
        issues = smwflow.compare.basic_compare(config, obj, git_value, smw_value)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""

    return []

def _verify_files(config, imps_vars, ctype, cname):
    cfgset_files_path = os.path.join(config.configset_path, cname, 'files')
    cfgset_files_path = os.path.realpath(cfgset_files_path)

    # first, walk the cfgset and build a list of all existing paths
    start_idx = len(cfgset_files_path) + 1
    cfgset_files = {}
    for (dirpath, dirnames, filenames) in os.walk(cfgset_files_path):
        for path in filenames:
            relpath =  os.path.join(dirpath, path)[start_idx:]
            cfgset_files[relpath] = { 'match': False, 'checked': False, 'smwpath': os.path.join(dirpath, path) }

    # next, walk the repos and discover a list of objects
    objs = smwflow.search.get_objects(config, 'imps', 'files', ctype)

    # execute any discovered files smwflow plugins, generate content in-memory
    ## TODO

    smw_keys = set(cfgset_files.keys())
    git_keys = set(objs.keys())

    smwonly_keys = smw_keys - git_keys
    gitonly_keys = git_keys - smw_keys
    common_keys = smw_keys.intersection(git_keys)

    if len(smwonly_keys) > 0:
        print "files only on smw: ", smwonly_keys
        print

    if len(gitonly_keys) > 0:
        print  "files only in git: ", gitonly_keys
        print

    for key in common_keys:
        obj = objs[key]
        obj['smwpath'] = cfgset_files[key]['smwpath']
        smw_value = None
        git_value = None

        with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
            git_value = rfp.read()
        with codecs.open(obj['smwpath'], mode='r', encoding='utf-8') as rfp:
            smw_value = rfp.read()
        issues = smwflow.compare.basic_compare(config, obj, git_value, smw_value)
        if len(issues) > 0:
            print "DIFFERENCES FOUND IN %s" % obj['name']
            for item in issues:
                print item
            print ""

    return []

def _init_cfgset(config, ctype, cname, imps_vars):
    cfgset_path = os.path.join(config.configset_path, cname)
    if os.path.exists(cfgset_path):
        print("cfgset path %s already exists" % cfgset_path)
        sys.exit(1)

    print("Initializing empty cfgset")
    retc = 0
    command = [
        "cfgset",
        "create",
        "--mode=prepare",
        "--type=%s" % ctype,
        "--no-scripts", cname,
    ]
    retc = subprocess.call(command)
    if retc != 0:
        print("FAILED to init cfgset %s" % cname)
        sys.exit(1)

def verify_data(config):
    deferred_actions = []

    ctype = config.cfgset_type
    cname = config.cfgset_name

    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)

    deferred_actions.extend(_verify_worksheets(config, imps_vars, ctype, cname))
    deferred_actions.extend(_verify_configs(config, imps_vars, ctype, cname))
    deferred_actions.extend(_verify_dist_preload(config, imps_vars, ctype, cname))
    deferred_actions.extend(_verify_ansible(config, imps_vars, ctype, cname))
    deferred_actions.extend(_verify_files(config, imps_vars, ctype, cname))

    return deferred_actions

def create(config):
    deferred_actions = []

    ctype = config.cfgset_type
    cname = config.cfgset_name

    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)

    ## step 1 initialize empty cfgset
    _init_cfgset(config, ctype, cname, imps_vars)

    return deferred_actions
