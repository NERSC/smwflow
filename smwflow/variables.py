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
import yaml
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
                print "WARNING: cannot read %s, no usable ansible hash" % encrypted_vars_path
                continue
            with open(encrypted_vars_path, 'r') as rfp:
                try:
                    data = yaml.load(config.vaultobj.decrypt(rfp.read()))
                except:
                    print "Cannot decrypt variables in %s; skipping" % encrypted_vars_path
                    continue
                for key in data:
                    variables[key] = data[key]
    if parentvars:
        for key in parentvars:
            if key not in variables:
                variables[key] = parentvars[key]

    return variables
