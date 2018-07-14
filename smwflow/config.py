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

"""
smwflow.config

smwflow configuration file and argument parsers to generate configurations
used throughout the other smwflow modules.
"""

import os
import argparse
import ConfigParser
import codecs
import json
import ansible.utils.vault as vault
import smwflow
import smwflow.search
import smwflow.variables

class BaseConfig(dict):
    """Class representing the smwflow configuration fileself.

    The smwflow configuration file contains base configurations and paths, most
    of which can be overridden on the command line, but is used by the administrator
    or user to provide reasonable defaults for their development environment.
    """

    def __init__(self):
        super(BaseConfig, self).__init__()

        system = None
        if os.path.exists('/etc/clustername'):
            with open('/etc/clustername') as rfp:
                system = rfp.read().strip()

        defaults = {
            'smwconf': '%s/smwconf' % smwflow.DEFAULT_GIT_BASEPATH,
            'secured': '%s/secured' % smwflow.DEFAULT_GIT_BASEPATH,
            'zypper': '%s/zypper' % smwflow.DEFAULT_GIT_BASEPATH,
            'system': system,
            'password_file': "",
            'configset_path': '/var/opt/cray/imps/config/sets',
            'partition': 'p0',
            'platform_json': None,
        }

        config_fname = '%s/smwflow.conf' % smwflow.CONFIG_PATH
        parser = ConfigParser.SafeConfigParser(defaults)
        parser.read([config_fname, os.path.expanduser('~/.smwflow.conf')])
        self['smwconf'] = parser.get('smwflow', 'smwconf')
        self['secured'] = parser.get('smwflow', 'secured')
        self['zypper'] = parser.get('smwflow', 'zypper')
        self['system'] = parser.get('smwflow', 'system')
        self['password_file'] = parser.get('smwflow', 'password_file')
        self['configset_path'] = parser.get('smwflow', 'configset_path')
        self['partition'] = parser.get('smwflow', 'partition')
        self['platform_json'] = parser.get('smwflow', 'platform_json')

class ArgCheckoutBranchAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super(ArgCheckoutBranchAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not getattr(namespace, 'smwconf_branch', None):
            setattr(namespace, 'smwconf_branch', values)
        if not getattr(namespace, 'secured_branch', None):
            setattr(namespace, 'secured_branch', values)
        if not getattr(namespace, 'zypper_branch', None):
            setattr(namespace, 'zypper_branch', values)

class ArgConfig(object):
    """Class merging user-provided and BaseConfig values for the instance configuration.

    Uses argparse extensively to intepret user desires for each mode of operation.
    The resulting configuration is used by smwflow.process and all other smwflow
    modules to carry out any and all actions smwflow is capable of taking.
    """
    def __init__(self, config, argv):
        super(ArgConfig, self).__init__()
        self.parser = self.__get_parser(config)
        self.values = self.parser.parse_args(argv)
        if not config['password_file']:
            config['password_file'] = os.path.join(config['secured'], 'ansible_vault/ansible.hash')
        if os.path.exists(config['password_file']):
            with open(config['password_file'], 'r') as rfp:
                password = rfp.read().strip()
                vaultobj = vault.VaultLib(password)
                setattr(self.values, 'vaultobj', vaultobj)
        print self.values
        variables = smwflow.variables.read_vars(self.values, 'vars', 'vars')
        setattr(self.values, "global_vars", variables)

    def parse_platform(self):
        """Reads p0.platform.json and generates a dictionary mapping the platform
           name to its constituent host cnames.

        Assumes that each platform key begins with "platform:" and each value
        is a list of cnames.

        TODO: consider using rsm.node_groups.resolvers.cle instead
        """

        platform_json = self.values.platform_json
        if not platform_json:
            filename = '%s.platform.json' % self.values.partition
            platform_json = os.path.join(self.values.configset_path,
                                         'global/files/node_groups/platforms',
                                         filename)

        with codecs.open(platform_json, mode='r', encoding='utf-8') as rfp:
            platform = json.load(rfp)

        platform = {}
        for p_name, p_nodes in platform.items():
            if p_name.startswith('platform:'):
                platform[p_name] = [p_node for p_node in p_nodes]

        return platform

    def __get_parser(self, config):
        parser = argparse.ArgumentParser(description='smwflow git-based Cray systems management')
        parser.add_argument('--smwconf', default=config['smwconf'],
                            help='path to the primary smw configuration git repo')
        parser.add_argument('--secured', default=config['secured'],
                            help='path to the secured smw configuration git repo')
        parser.add_argument('--zypper', default=config['zypper'],
                            help='path to zypper git-lfs repo')
        parser.add_argument('--system', default=config['system'],
                            help='system name')
        parser.add_argument('--configset_path', default=config['configset_path'],
                            help='root path to config sets')
        parser.add_argument('--password_file', default=config['password_file'],
                            help='Ansible vault password file')
        parser.add_argument('--partition', default=config['partition'],
                            help='XC partition for configuration')
        self.subparsers = parser.add_subparsers(help='smwflow command')

        self._setup_status_parser()
        self._setup_checkout_parser()
        self._setup_update_parser()
        self._setup_verify_parser()
        self._setup_create_parser()
        self._setup_import_parser()
        return parser

    def _setup_status_parser(self):
        p_status = self.subparsers.add_parser('status', help='smwflow status')
        p_status.set_defaults(mode='status')
        return p_status

    def _setup_checkout_parser(self):
        p_checkout = self.subparsers.add_parser('checkout',
                                                help='checkout named branch in all repos')
        p_checkout.set_defaults(mode='checkout')
        p_checkout.add_argument('--smwconf', help='override branch for smwconf repo',
                                dest='smwconf_branch')
        p_checkout.add_argument('--secured', help='override branch for smwconf repo',
                                dest='secured_branch')
        p_checkout.add_argument('--zypper', help='override branch for smwconf repo',
                                dest='zypper_branch')
        p_checkout.add_argument('--pull', help='perform git-pull in each repo',
                                default=False, dest='checkout_pull', action='store_true')
        p_checkout.add_argument('branch', help='name of branch to use',
                                action=ArgCheckoutBranchAction)
        return p_checkout

    def _setup_update_parser(self):
        p_update = self.subparsers.add_parser('update', help='update smw configurations')
        p_update.set_defaults(mode='update', update_hss=False, update_basesmw=False,
                              update_cfgset=False, update_both_cfgset=False,
                              update_imps=False, update_zypper=False)
        p_update.add_argument('--dry-run', help='do not actually modify anything, just pretend',
                              default=False, action='store_true')
        p_update_sp = p_update.add_subparsers(help='update smw configurations from smw')

        p_update_all = p_update_sp.add_parser('all', help='update all smw possible '
                                              'smw configurations')
        p_update_all.set_defaults(update_imps=True, update_hss=True, update_basesmw=True,
                                  update_both_cfgset=True, update_cfgset=True)
        p_update_all.add_argument('--cle_configset', default='p0', help='name of cle configset')

        p_update_imps = p_update_sp.add_parser('imps', help='update image management and '
                                               'provisioning system (imps)')
        p_update_imps.set_defaults(update_imps=True)

        p_update_hss = p_update_sp.add_parser('hss', help='update hss configurations')
        p_update_hss.set_defaults(update_hss=True)

        p_update_basesmw = p_update_sp.add_parser('basesmw', help='update base smw configurations')
        p_update_basesmw.set_defaults(update_basesmw=True)

        p_update_cfgset = p_update_sp.add_parser('cfgset', help='update configset')
        p_update_cfgset.set_defaults(update_cfgset=True)
        p_update_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'],
                                     default='cle', dest='cfgset_type')
        p_update_cfgset.add_argument('cfgset_name', help='configset name')

        p_update_zypper = p_update_sp.add_parser('zypper', help='update zypper repos')
        p_update_zypper.set_defaults(update_zypper=True)
        p_update_zypper.add_argument('--all', help='setup all git-defined zypper repos',
                                     default=False, action='store_true', dest='update_all_zypper')
        p_update_zypper.add_argument('zypper_repos', nargs='*',
                                     help='specific zypper repos to update')
        return p_update

    def _setup_verify_parser(self):
        p_verify = self.subparsers.add_parser('verify', help='verify smw configurations')
        p_verify.set_defaults(mode='verify', verify_imps=False, verify_hss=False,
                              verify_basesmw=False, verify_cfgset=False,
                              verify_both_cfgset=False, verify_zypper=False,
                              verify_all_zypper=False)
        p_verify_sp = p_verify.add_subparsers(help='verify smw configurations')
        p_verify_all = p_verify_sp.add_parser('all', help='verify all smw configurations')
        p_verify_all.set_defaults(verify_imps=True, verify_hss=True, verify_basesmw=True,
                                  verify_both_cfgset=True)
        p_verify_all.add_argument('--cle_configset', default='p0', help='name of cle '
                                  'configset')

        p_verify_imps = p_verify_sp.add_parser('imps', help='verify image management '
                                               'and provisioning system (imps)')
        p_verify_imps.set_defaults(verify_imps=True)

        p_verify_hss = p_verify_sp.add_parser('hss', help='verify hss configurations')
        p_verify_hss.set_defaults(verify_hss=True)

        p_verify_basesmw = p_verify_sp.add_parser('basesmw', help='update base smw configurations')
        p_verify_basesmw.set_defaults(verify_basesmw=True)

        p_verify_cfgset = p_verify_sp.add_parser('cfgset', help='verify configset')
        p_verify_cfgset.set_defaults(verify_cfgset=True)
        p_verify_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'],
                                     default='cle', dest='cfgset_type')
        p_verify_cfgset.add_argument('cfgset_name', help='configset name')

        p_verify_zypper = p_verify_sp.add_parser('zypper', help='verify zypper repos')
        p_verify_zypper.set_defaults(verify_zypper=True)
        p_verify_zypper.add_argument('--all', help='verify all git-defined zypper repos',
                                     default=False, action='store_true', dest='verify_all_zypper')
        p_verify_zypper.add_argument('zypper_repos', nargs='*',
                                     help='specific zypper repos to verify')
        return p_verify

    def _setup_create_parser(self):
        p_create = self.subparsers.add_parser('create', help='create new smw configurations')
        p_create.set_defaults(mode='create', create_cfgset=False, create_zypper=False)
        p_create_sp = p_create.add_subparsers(help='create smw configurations')
        p_create_cfgset = p_create_sp.add_parser('cfgset', help='create configset')
        p_create_cfgset.set_defaults(create_cfgset=True)
        p_create_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'],
                                     default='cle', dest='cfgset_type')
        p_create_cfgset.add_argument('cfgset_name', help='configset name')
        return p_create

    def _setup_import_parser(self):
        p_import = self.subparsers.add_parser('import', help='import smw configurations '
                                              'into git repos, will need to add and commit '
                                              'changes following operation')
        p_import.set_defaults(mode='import', import_imps=False, import_hss=False,
                              import_basesmw=False, import_both_cfgset=False)
        p_import_sp = p_import.add_subparsers(help='import smw configurations')
        p_import_all = p_import_sp.add_parser('all', help='import all smw configurations')
        p_import_all.set_defaults(import_imps=True, import_hss=True, import_basesmw=True,
                                  import_both_cfgset=True)
        p_import_all.add_argument('--cle_configset', default='p0', help='name of cle configset')
        p_import_imps = p_import_sp.add_parser('imps', help='import image management '
                                               'and provisioning system (imps)')
        p_import_imps.set_defaults(import_imps=True)

        p_import_hss = p_import_sp.add_parser('hss', help='import hss configurations')
        p_import_hss.set_defaults(import_hss=True)

        p_import_basesmw = p_import_sp.add_parser('basesmw', help='update base smw configurations')
        p_import_basesmw.set_defaults(import_basesmw=True)

        p_import_cfgset = p_import_sp.add_parser('cfgset', help='import configset')
        p_import_cfgset.set_defaults(import_cfgset=True)
        p_import_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'],
                                     default='cle', dest='cfgset_type')
        p_import_cfgset.add_argument('cfgset_name', help='configset name')
        return p_import
