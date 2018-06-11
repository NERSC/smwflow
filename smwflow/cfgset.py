## Author: Douglas Jacobsen <dmjacobsen@lbl.gov>

import os
import sys
import stat
import subprocess
import tempfile
import codecs
import datetime
import socket
from jinja2 import Template
import yaml
import rsm.hss
import smwflow.compare
import smwflow.manifest
import smwflow.search
import smwflow.smwfile
import smwflow.variables


MANAGED_CFGSET_WORKSHEET = {
}

MANAGED_CFGSET_CONFIG = {
    'cray_lmt_config.yaml': {'mode': 0600},
    'cray_local_users_config.yaml': {'mode': 0600},
}

def _render_obj(obj, objtype_vars):
    git_data = None
    with codecs.open(obj['fullpath'], mode='r', encoding='utf-8') as rfp:
        data = rfp.read()
        template = Template(data)
        git_data = template.render(objtype_vars)
    return git_data

def _read_smw_obj(obj):
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

def _basic_verify(git_objs, smw_objs):
    ret = {'differences': 0}
    smw_keys = set(smw_objs.keys())
    git_keys = set(git_objs.keys())

    smwonly_keys = smw_keys - git_keys
    gitonly_keys = git_keys - smw_keys
    common_keys = smw_keys.intersection(git_keys)

    ret['keys_smw_only'] = sorted([x for x in smwonly_keys])
    ret['differences'] += len(ret['keys_smw_only'])
    ret['keys_git_only'] = sorted([x for x in gitonly_keys])
    ret['differences'] += len(ret['keys_git_only'])
    ret['value_diff'] = {}
    ret['permissions'] = []

    return ret, common_keys

def _filter_smw_worksheet(path):
    """ Identify worksheet objects of interest in an SMW config set.

    All worksheets should be considered for management by smwflow.  Just ensure
    that the proposed file is a regular file and ends with '_worksheet.yaml'

    Args:
        path (string): absolute path to a potential worksheet

    Returns: boolean
        True if the path should be considered a worksheet
        False if not.
    """
    stdata = os.stat(path)
    if stat.S_ISREG(stdata.st_mode) and path.endswith('_worksheet.yaml'):
        return True
    return False

def _filter_smw_config(path):
    """ Identify config objects of interest in an SMW config set.

    Most config objects in a config set are fully described by the worksheets
    and so should be ignored by this software.  Any config object not starting
    with "cray_" or the global config set "cray_image_groups.yaml" file should
    be considered.

    Args:
        path (string): absolute path to a potential config object

    Returns: boolean
        True if path should be considered a managed config object
        False if not.
    """
    stdata = os.stat(path)
    if not stat.S_ISREG(stdata.st_mode):
        return False
    _, fname = os.path.split(path)
    if fname == 'cray_image_groups.yaml':
        return False
    if fname == 'smwflow_metadata.yaml':
        return False
    if fname.startswith('cray_'):
        return False
    return True

def _filter_smw_dist_preload(path):
    stdata = os.stat(path)
    if not stat.S_ISREG(stdata.st_mode):
        return False
    _, fname = os.path.split(path)
    if fname.find('preload') >= 0 and fname.find('cray') < 0:
        return True
    return False

def __simple_worksheet_config(data):
    """Simple parser of Cray CMF worksheets.

    Assuming a Cray CMF yaml worksheet is parsed and the resulting dict is
    valid, this function breaks apart the complex keys into a hierarchy
    of dictionaries to enable easy traversal of the represented data structure.

    Args:
      data (dict)     : parsed and evaluated Cray CMF yaml worksheet

    Returns: dict
      dict containing complex representation of all keys in the worksheet.
      e.g., worksheet cray_something.data.managed = False would be represented
      here with ret['cray_something']['data']['managed'] = False
    """
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

class ConfigSet(object):
    def __init__(self, config, ctype, cname, parent_vars):
        self.config = config
        self.cfgset_type = ctype
        self.cfgset_name = cname
        self.parent_vars = parent_vars
        self.routermap = {}
        self.todo = []
        routermap = rsm.hss.RouterMap(self.config.partition)
        for node in routermap:
            self.routermap[node.cname] = node

    def smwimport(self):
        pass

    def verify(self):
        diff = {}
        diff['worksheets'] = self._verify_template_objs('worksheets',
                                                        _filter_smw_worksheet,
                                                        MANAGED_CFGSET_WORKSHEET)
        diff['config'] = self._verify_template_objs('config', _filter_smw_config,
                                                    MANAGED_CFGSET_CONFIG)
        diff['dist'] = self._verify_template_objs('dist', _filter_smw_dist_preload, None)
        diff['ansible'] = self._verify_filetree('ansible', None)
        diff['files'] = self._verify_filetree('files', None)

        return diff

    def update(self):
        self._modify_cfgset(True)
        self._validate_cfgset()

    def create(self):
        self._init_smw_cfgset()
        self._modify_cfgset(False)
        self._validate_cfgset()

    def parse_network(self, system=None):
        """Parse cray_net_worksheet for specified system.

        Renders and produces a functional cray_net_worksheet for the specified
        system even if it is not the current system.  This is necessary for
        generating some configurations that have global or multi-system reach
        (e.g., sshd).

        Args:
          self (ConfigSet): reference to current class instance
          system (string) : string defining target system for cray_net_worksheet.

        Returns:
          a dictionary representing the keys and values in the worksheet
          with all multipart (this.that.the.other: True) broken up into a multi
          level dictionary (ret['this']['that']['the']['other'] = True)
        """
        if not system:
            system = self.config.system

        objs = smwflow.search.get_objects(self.config, 'imps', 'worksheets',
                                          self.cfgset_type, system=system)
        global_vars = smwflow.variables.read_vars(self.config, 'vars', 'vars', system=system)
        imps_vars = smwflow.variables.read_vars(self.config, 'imps', 'vars', None,
                                                global_vars, system=system)
        worksheet_vars = smwflow.variables.read_vars(self.config, 'imps', 'worksheet_vars',
                                                     self.cfgset_type, imps_vars, system=system)

        net_worksheet = _render_obj(objs['cray_net_worksheet.yaml'], worksheet_vars)
        data = yaml.load(net_worksheet)
        return __simple_worksheet_config(data)

    def parse_nodegroups(self, system=None):
        if not system:
            system = self.config.system
        objs = smwflow.search.get_objects(self.config, 'imps', 'worksheets',
                                          self.cfgset_type, system=system)
        global_vars = smwflow.variables.read_vars(self.config, 'vars', 'vars',
                                                  system=system)
        imps_vars = smwflow.variables.read_vars(self.config, 'imps', 'vars',
                                                None, global_vars, system=system)
        worksheet_vars = smwflow.variables.read_vars(self.config, 'imps',
                                                     'worksheet_vars', self.cfgset_type,
                                                     imps_vars, system=system)

        worksheet = _render_obj(objs['cray_node_groups_worksheet.yaml'], worksheet_vars)
        data = yaml.load(worksheet)
        return __simple_worksheet_config(data)

    def _get_smw_objects(self, obj_type, filter_fxn, extra):
        smw_files = {}
        managed_smw_files = {}
        cfgset_path = os.path.join(self.config.configset_path, self.cfgset_name)
        if not os.path.exists(cfgset_path):
            raise ValueError('cfgset %s does not exist' % cfgset_path)
        obj_path = os.path.join(cfgset_path, obj_type)
        filenames = os.listdir(obj_path)
        for filename in filenames:
            path = os.path.join(obj_path, filename)
            if filter_fxn(self, path):
                smw_files[filename] = {'smwpath': path}
                if extra and filename in extra:
                    for key in extra[filename]:
                        if key not in smw_files[filename]:
                            smw_files[filename][key] = extra[filename][key]
        if extra:
            for filename in extra:
                obj = {'smwpath': os.path.join(obj_path, filename)}
                for key in extra[filename]:
                    obj[key] = extra[filename][key]
                managed_smw_files[filename] = obj
        return smw_files, managed_smw_files

    def _get_smw_obj_filetree(self, obj_type, filter_fxn):
        """ Walk a filetree for a specified object type in the config set.

        Generate a dicitonary of path references for tracking comparable relative
        paths and real paths on the SMW for the current config set.  The
        filter_fxn variable is currently unused but specified in the API because
        it is assumed it will be adopted shortly, and is needed for other similar
        functions like _get_smw_objects.

        Args:
            self (ConfigSet):       reference to current config set
            obj_type (string):      type of cfgset object to consider, current
                                    support for 'files' and 'ansible'
            filter_fxn (function):  currently unused, but will be used to screen
                                    out irrelevant content (i.e., things allowed
                                    to be auto-generated by cray software in the
                                    object subtype of the config set)

        Returns: dict
            Keys are relative paths to ensure comparability between the
            git and smw objects.  Values are dictionaries containing the key
            "smwpath" pointing to the absolute path on the smw.  Other entries
            are unused and may be removed in the future.

            Example:
            {
                "readme.txt":
                    {"smwpath": "/var/opt/cray/imps/config/sets/p0/ansible/readme.txt"},
                "roles/test/tasks/main.yaml":
                    "/var/opt/cray/imps/config/sets/p0/ansible/roles/test/tasks/main.yaml"},
            }
        """
        cfgset_path = os.path.join(self.config.configset_path, self.cfgset_name, obj_type)
        cfgset_path = os.path.realpath(cfgset_path)

        # walk the cfgset and build a list of all existing paths
        start_idx = len(cfgset_path) + 1
        cfgset_objs = {}
        for (dirpath, _, filenames) in os.walk(cfgset_path):
            for path in filenames:
                relpath = os.path.join(dirpath, path)[start_idx:]
                cfgset_objs[relpath] = {
                    'match': False,
                    'checked': False,
                    'smwpath': os.path.join(dirpath, path)
                }
        return cfgset_objs

    def _get_template_objs(self, obj_type, extra):
        git_objs, = smwflow.search.get_objects(self.config, 'imps', obj_type, self.cfgset_type)
        local_vars = smwflow.variables.read_vars(self.config, 'imps',
                                                 '%s_vars' % obj_type, self.cfgset_type,
                                                 self.parent_vars)
        # execute any discovered ansible smwflow plugins, generate content in-memory
        ## # TODO

        for filename in git_objs:
            obj = git_objs[filename]
            obj['name'] = filename
            if 'data' not in obj:
                obj['data'] = _render_obj(obj, local_vars)
            if extra and filename in extra:
                for key in extra[filename]:
                    if key not in obj:
                        obj[key] = extra[filename][key]

        return git_objs

    def _verify_template_objs(self, obj_type, filter_fxn, extra):

        smw_objs, managed_smw_objs = self._get_smw_objects(obj_type, filter_fxn, extra)
        git_objs, = smwflow.search.get_objects(self.config, 'imps', obj_type, self.cfgset_type)
        local_vars = smwflow.variables.read_vars(self.config, 'imps',
                                                 '%s_vars' % obj_type, self.cfgset_type,
                                                 self.parent_vars)

        # execute any discovered ansible smwflow plugins, generate content in-memory
        ## # TODO
        ret, common_keys = _basic_verify(git_objs, smw_objs)

        for key in common_keys:
            obj = git_objs[key]
            obj['name'] = key
            obj['smwpath'] = smw_objs[key]['smwpath']

            git_data = _render_obj(obj, local_vars)
            smw_data = _read_smw_obj(obj)

            tmp = smwflow.compare.basic_compare(self.config, obj, git_data, smw_data)
            if tmp:
                ret['value_diff'][key] = tmp
                ret['differences'] += len(tmp)

            if not smwflow.smwfile.verifyattributes(self.config, obj):
                ret['permissions'].append(obj['smwpath'])
                ret['differences'] += 1

        for key in managed_smw_objs:
            obj = managed_smw_objs[key]
            if not smwflow.smwfile.verifyattributes(self.config, obj) \
                    and obj['smwpath'] not in ret['permissions']:

                ret['permissions'].append(obj['smwpath'])
                ret['differences'] += 1

        return ret

    def _verify_filetree(self, obj_type, filter_fxn):
        smw_objs = self._get_smw_obj_filetree(obj_type, filter_fxn)
        git_objs = smwflow.search.get_objects(self.config, 'imps', obj_type, self.cfgset_type)

        # execute any discovered ansible smwflow plugins, generate content in-memory
        ## TODO

        ret, common_keys = _basic_verify(git_objs, smw_objs)

        for key in common_keys:
            obj = git_objs[key]
            obj['smwpath'] = smw_objs[key]['smwpath']
            smw_value = None
            git_value = None

            with codecs.open(git_objs[key]['fullpath'], mode='r', encoding='utf-8') as rfp:
                git_value = rfp.read()
            with codecs.open(git_objs[key]['smwpath'], mode='r', encoding='utf-8') as rfp:
                smw_value = rfp.read()
            tmp = smwflow.compare.basic_compare(self.config, obj, git_value, smw_value)
            if tmp:
                ret['value_diff'][key] = tmp
                ret['differences'] += len(tmp)
            if not smwflow.smwfile.verifyattributes(self.config, obj):
                ret['permissions'].append(obj['smwpath'])
                ret['differences'] += 1

        return ret

    def _modify_cfgset(self, do_verify):
        self.todo = []
        self._setup_worksheets(do_verify=do_verify)
        self._setup_config(do_verify=do_verify)
        self._setup_dist(do_verify=do_verify)
        self._setup_files(do_verify=do_verify)
        self._setup_ansible(do_verify=do_verify)
        self._setup_metadata()
        while self.todo:
            fxn = self.todo.pop()
            fxn()

    def _init_smw_cfgset(self):
        cfgset_path = os.path.join(self.config.configset_path, self.cfgset_name)
        if os.path.exists(cfgset_path):
            print "cfgset path %s already exists" % cfgset_path
            sys.exit(1)

        print "Initializing empty cfgset"
        retc = 0
        command = [
            "cfgset",
            "create",
            "--mode=prepare",
            "--type=%s" % self.cfgset_type,
            "--no-scripts", self.cfgset_name,
        ]
        retc = subprocess.call(command)
        if retc != 0:
            print "FAILED to init cfgset %s" % self.cfgset_name
            sys.exit(1)

    def _setup_worksheets(self, do_verify=False):
        if do_verify:
            diffs = self._verify_template_objs('worksheets', _filter_smw_worksheet,
                                               MANAGED_CFGSET_WORKSHEET)
            if diffs['differences'] == 0:
                return 0

        worksheets = self._get_template_objs('worksheets', MANAGED_CFGSET_WORKSHEET)
        worksheets_tmp = tempfile.mkdtemp()
        for key in worksheets:
            wpath = os.path.join(worksheets_tmp, key)
            with open(wpath, 'w') as wfp:
                wfp.write(worksheets[key]['data'])

        print "Updating cfgset with worksheets"
        command = [
            "cfgset",
            "update",
            "--mode=prepare",
            "--no-scripts",
            "-w",
            '%s/*yaml' % worksheets_tmp,
            self.cfgset_name
        ]
        retc = subprocess.call(command)
        self.todo.append(self._update_cfgset)

        cfgset_wks_root = os.path.join(self.config.configset_path, self.cfgset_name, 'worksheets')
        for key in worksheets:
            obj = worksheets[key]
            obj['smwpath'] = os.path.join(cfgset_wks_root, key)
            smwflow.smwfile.setattributes(self.config, obj)
        for key in MANAGED_CFGSET_WORKSHEET:
            obj = {'smwpath': os.path.join(cfgset_wks_root, key)}
            smwflow.smwfile.setattributes(self.config, obj)

        return retc

    def _setup_simple_obj(self, obj_type, do_verify=False, filter_fxn=None, extra=None):
        if do_verify:
            diffs = self._verify_template_objs(obj_type, filter_fxn, extra)
            if diffs['differences'] == 0:
                return 0
        cfgset_obj_root = os.path.join(self.config.configset_path, self.cfgset_name, obj_type)
        objs = self._get_template_objs(obj_type, extra)
        for key in objs:
            wpath = os.path.join(cfgset_obj_root, key)
            objs[key]['smwpath'] = wpath
            with open(wpath, 'w') as wfp:
                wfp.write(objs[key]['data'])
            smwflow.smwfile.setattributes(self.config, objs[key])
        return 0

    def _setup_config(self, do_verify=False):
        return self._setup_simple_obj('config', do_verify, _filter_smw_config,
                                      MANAGED_CFGSET_CONFIG)

    def _setup_dist(self, do_verify=False):
        return self._setup_simple_obj('dist', do_verify, _filter_smw_dist_preload, None)

    def _setup_filetree_obj(self, obj_type, do_verify=False, filter_fxn=None, extra=None):
        git_objs = smwflow.search.get_objects(self.config, 'imps', obj_type, self.cfgset_type)
        local_vars = smwflow.variables.read_vars(self.config, 'imps',
                                                 '%s_vars' % obj_type, self.cfgset_type,
                                                 self.parent_vars)
        # execute any discovered ansible smwflow plugins, generate content in-memory
        ## # TODO

        ftree_root = os.path.join(self.config.configset_path, self.cfgset_name, obj_type)

        dirs = set()
        # pass 1, setup obj and build directory map
        for filename in git_objs:
            obj = git_objs[filename]
            obj['name'] = filename
            obj['smwpath'] = os.path.join(ftree_root, filename)
            if extra and filename in extra:
                for key in extra[filename]:
                    if key not in obj:
                        obj[key] = extra[filename][key]
            if 'isdirectory' not in obj:
                obj['isdirectory'] = False

            components = filename.split('/')
            if not obj['isdirectory']:
                components = components[:-1]

            for idx in xrange(1, len(components) + 1):
                currpath = '/'.join(components[:idx])
                dirs.add(currpath)

        # generate sorted list of directories so we can create them in sequence
        # set umask so os.mkdir() sets the modes correctly
        sv_umask = os.umask(0)
        dirs = sorted(list(dirs))
        for dirname in dirs:
            smwpath = os.path.join(ftree_root, dirname)
            mode = 0755
            if dirname in git_objs and 'mode' in git_objs[dirname]:
                mode = git_objs[dirname]['mode']
            try:
                os.mkdir(smwpath, mode)
            except OSError:
                pass
        os.umask(sv_umask)

        for filename in git_objs:
            if obj['isdirectory']:
                continue
            if 'data' not in obj:
                obj['data'] = _render_obj(obj, local_vars)
            wpath = obj['smwpath']
            with open(wpath, 'w') as wfp:
                wfp.write(obj['data'])
            mode = obj['mode'] if 'mode' in obj else 0644
            os.chmod(wpath, mode)
            smwflow.smwfile.setattributes(self.config, obj)

    def _setup_files(self, do_verify=False):
        return self._setup_filetree_obj('files', do_verify, None, None)

    def _setup_ansible(self, do_verify=False):
        return self._setup_filetree_obj('ansible', do_verify, None, None)

    def _setup_metadata(self):
        metadata = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        metadata["smwconf"] = {
            "HEAD": smwflow.process.get_git_head_rev(self.config.smwconf),
            "branch": smwflow.process.get_git_branch(self.config.smwconf),
            "path": self.config.smwconf
        }
        metadata["secured"] = {
            "HEAD": smwflow.process.get_git_head_rev(self.config.secured),
            "branch": smwflow.process.get_git_branch(self.config.secured),
            "path": self.config.secured
        }
        metadata['construct_cfgset_config'] = self.config
        metadata['build_host'] = socket.gethostname()

        config_path = os.path.join(self.config.configset_root, self.cfgset_name, 'config')
        metadata_path = os.path.join(config_path, 'smwflow_metadata.yaml')
        with open(metadata_path, 'w') as wfp:
            wfp.write(yaml.dump(metadata))
            wfp.close()

    def _update_cfgset(self):
        print "Update config set %s" % self.cfgset_name
        command = [
            "cfgset",
            "update",
            "--mode=prepare",
        ]
        if self.config.noscripts:
            command.append("--no-scripts")
        command.append(self.cfgset_name)
        retc = subprocess.call(command)
        return retc

    def _validate_cfgset(self):
        print "Validate config set %s" % self.cfgset_name
        command = [
            "cfgset",
            "validate",
            self.cfgset_name
        ]
        retc = subprocess.call(command)
        diffs = self.verify()
        return retc + diffs['differences']

    def display_diffs(self, diffs):
        pass

def verify_data(config):
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)

    if config.verify_both_cfgset:
        global_cfgset = ConfigSet(config, 'global', 'global', imps_vars)
        cle_cfgset = ConfigSet(config, 'cle', config.cle_configset, imps_vars)
        global_diffs = global_cfgset.verify()
        cle_diffs = cle_cfgset.verify()

        if global_diffs['differences'] > 0:
            global_cfgset.display_diffs(global_diffs)
        if cle_diffs['differences'] > 0:
            cle_cfgset.display_diffs(cle_diffs)
        return []
    configset = ConfigSet(config, config.cfgset_type, config.cfgset_name, imps_vars)
    diffs = configset.verify()
    if diffs['differences'] > 0:
        configset.display_diffs(cle_diffs)
    return []

def create(config):
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)
    configset = ConfigSet(config, config.cfgset_type, config.cfgset_name, imps_vars)
    configset.create()
    return []

def update_data(config):
    imps_vars = smwflow.variables.read_vars(config, 'imps', 'vars', None, config.global_vars)
    configset = ConfigSet(config, config.cfgset_type, config.cfgset_name, imps_vars)
    configset.update()
    return []
