import os
import sys
import argparse
import ConfigParser
import smwflow
import smwflow.search
import smwflow.variables

class Config(object):
    def __init__(self):
        system = None
        if os.path.exists('/etc/clustername'):
            with open('/etc/clustername') as fp:
                system = fp.read().strip()
            
        defaults = {
            'smwconf': '%s/smwconf' % smwflow.DEFAULT_GIT_BASEPATH,
            'secured': '%s/secured' % smwflow.DEFAULT_GIT_BASEPATH,
            'zypper': '%s/zypper' % smwflow.DEFAULT_GIT_BASEPATH,
            'system': system,
            'password_file': ""
        }

        config_fname = '%s/smwflow.conf' % smwflow.CONFIG_PATH
        parser = ConfigParser.SafeConfigParser(defaults)
        parser.read([config_fname, os.path.expanduser('~/.smwflow.conf')])
        self.smwconf = parser.get('smwflow', 'smwconf')
        self.secured = parser.get('smwflow', 'secured')
        self.zypper = parser.get('smwflow', 'zypper')
        self.system = parser.get('smwflow', 'system')
        self.password_file = parser.get('smwflow', 'password_file')

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
    def __init__(self, config, argv):
        parser = self.__get_parser(config)
        self.values = parser.parse_args(argv)
        if not config.password_file:
            config.password_file = os.path.join(config.secured, 'ansible_vault/ansible.hash')
        if os.path.exists(config.password_file):
            with open(config['password_file'], 'r') as rfp:
                password = rfp.read().strip()
                vaultobj = vault.VaultLib(password)
                setattr(self.values, 'vaultobj', vaultobj)     
        variables = smwflow.variables.read_vars(self.values, 'vars', 'vars')
        setattr(self.values, "global_vars", variables)

    def __get_parser(self, config):
        parser = argparse.ArgumentParser(description='smwflow git-based Cray systems management')
        parser.add_argument('--smwconf', default=config.smwconf, help='path to the primary smw configuration git repo')
        parser.add_argument('--secured', default=config.secured, help='path to the secured smw configuration git repo')
        parser.add_argument('--zypper', default=config.zypper, help='path to zypper git-lfs repo')
        parser.add_argument('--system', default=config.system, help='system name')
        parser.add_argument('--password_file', default=config.password_file, help='Ansible vault password file')
        subparsers = parser.add_subparsers(help='smwflow command')

        p_status = subparsers.add_parser('status', help='smwflow status')
        p_status.set_defaults(mode='status')

        p_checkout = subparsers.add_parser('checkout', help='checkout named branch in all repos')
        p_checkout.set_defaults(mode='checkout')
        p_checkout.add_argument('--smwconf', help='override branch for smwconf repo', dest='smwconf_branch')
        p_checkout.add_argument('--secured', help='override branch for smwconf repo', dest='secured_branch')
        p_checkout.add_argument('--zypper', help='override branch for smwconf repo', dest='zypper_branch')
        p_checkout.add_argument('--pull', help='perform git-pull in each repo', default=False, dest='checkout_pull', action='store_true')
        p_checkout.add_argument('branch', help='name of branch to use', action=ArgCheckoutBranchAction)

        p_update = subparsers.add_parser('update', help='update smw configurations')
        p_update.set_defaults(mode='update')
        p_update.add_argument('--dry-run', help='do not actually modify anything, just pretend', default=False, action='store_true')
        p_update_sp = p_update.add_subparsers(help='update smw configurations from smw')

        p_update_all = p_update_sp.add_parser('all', help='update all smw possible smw configurations')
        p_update_all.set_defaults(update_imps=True, update_hss=True, update_basesmw=True, update_both_cfgset=True)
        p_update_all.add_argument('--cle_configset', default='p0', help='name of cle configset')

        p_update_imps = p_update_sp.add_parser('imps', help='update image management and provisioning system (imps)')
        p_update_imps.set_defaults(update_imps=True)

        p_update_hss = p_update_sp.add_parser('hss', help='update hss configurations')
        p_update_hss.set_defaults(update_hss=True)

        p_update_basesmw = p_update_sp.add_parser('basesmw', help='update base smw configurations')
        p_update_basesmw.set_defaults(update_basesmw=True)

        p_update_cfgset = p_update_sp.add_parser('cfgset', help='update configset')
        p_update_cfgset.set_defaults(update_cfgset=True)
        p_update_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'], default='cle', dest='cfgset_type')
        p_update_cfgset.add_argument('cfgset_name', help='configset name')

        p_verify = subparsers.add_parser('verify', help='verify smw configurations')
        p_verify.set_defaults(mode='verify')
        p_verify_sp = p_verify.add_subparsers(help='verify smw configurations')
        p_verify_all = p_verify_sp.add_parser('all', help='verify all smw configurations')
        p_verify_all.set_defaults(verify_imps=True, verify_hss=True, verify_basesmw=True, verify_both_cfgset=True)
        p_verify_all.add_argument('--cle_configset', default='p0', help='name of cle configset') 
        p_verify_imps = p_verify_sp.add_parser('imps', help='verify image management and provisioning system (imps)')
        p_verify_imps.set_defaults(verify_imps=True)

        p_verify_hss = p_verify_sp.add_parser('hss', help='verify hss configurations')
        p_verify_hss.set_defaults(verify_hss=True)

        p_verify_basesmw = p_verify_sp.add_parser('basesmw', help='update base smw configurations')
        p_verify_basesmw.set_defaults(verify_basesmw=True)

        p_verify_cfgset = p_verify_sp.add_parser('cfgset', help='verify configset')
        p_verify_cfgset.set_defaults(verify_cfgset=True)
        p_verify_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'], default='cle', dest='cfgset_type')
        p_verify_cfgset.add_argument('cfgset_name', help='configset name')
        
        p_create = subparsers.add_parser('create', help='create new smw configurations')
        p_create.set_defaults(mode='create')
        p_create_sp = p_create.add_subparsers(help='create smw configurations')
        p_create_cfgset = p_create_sp.add_parser('cfgset', help='create configset')
        p_create_cfgset.set_defaults(create_cfgset=True)
        p_create_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'], default='cle', dest='cfgset_type')
        p_create_cfgset.add_argument('cfgset_name', help='configset name')

        p_import = subparsers.add_parser('import', help='import smw configurations into git repos, will need to add and commit changes following operation')
        p_import.set_defaults(mode='import')
        p_import_sp = p_import.add_subparsers(help='import smw configurations')
        p_import_all = p_import_sp.add_parser('all', help='import all smw configurations')
        p_import_all.set_defaults(import_imps=True, import_hss=True, import_basesmw=True, import_both_cfgset=True)
        p_import_all.add_argument('--cle_configset', default='p0', help='name of cle configset') 
        p_import_imps = p_import_sp.add_parser('imps', help='import image management and provisioning system (imps)')
        p_import_imps.set_defaults(import_imps=True)

        p_import_hss = p_import_sp.add_parser('hss', help='import hss configurations')
        p_import_hss.set_defaults(import_hss=True)

        p_import_basesmw = p_import_sp.add_parser('basesmw', help='update base smw configurations')
        p_import_basesmw.set_defaults(import_basesmw=True)

        p_import_cfgset = p_import_sp.add_parser('cfgset', help='import configset')
        p_import_cfgset.set_defaults(import_cfgset=True)
        p_import_cfgset.add_argument('--type', help='configset type', choices=['cle', 'global'], default='cle', dest='cfgset_type')
        p_import_cfgset.add_argument('cfgset_name', help='configset name')
        return parser
