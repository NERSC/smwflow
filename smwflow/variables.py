import os
import sys
import smwflow.search

def read_vars(config, maintype, objtype, subtype=None, parentvars=None, system=None):
    if not system:
        system = config.system

    vars_paths = smwflow.search.gen_paths(config, maintype, objtype, subtype)
    variables = {}
    for vars_path in vars_paths:
        unencrypted_vars_path = os.path.join(vars_path, '%s.yaml' % system)
        encrypted_vars_path = os.path.join(vars_path, '%s_secrets.yaml' % system)
        if os.path.exists(unencrypted_vars_path):
            with open(unencrypted_vars_path, 'r') as rfp:
                data = yaml.load(rfp.read())
                for key in data:
                    variables[key] = data[key]
        if os.path.exists(encrypted_vars_path) and os.access(encrypted_vars_path, os.R_OK):
            if not config.vaultobj:
                print("WARNING: cannot read %s, no usable ansible hash" % encrypted_vars_path)
                continue
            with open(encrypted_vars_path, 'r') as rfp:
                try:
                    data = yaml.load(config.vaultobj.decrypt(rfp.read()))
                except:
                    print("Cannot decrypt variables in %s; skipping" % encrypted_vars_path)
                    continue
                for key in data:
                    variables[key] = data[key]
    if parentvars:
        for key in parentvars:
            if key not in variables:
                variabels[key] = parentvars[key]

    return variables

