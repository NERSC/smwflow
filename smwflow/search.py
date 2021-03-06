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
import smwflow
import smwflow.manifest

def gen_paths(config, maintype, objtype, subtype=None, repos=('smwconf', 'secured'), system=None):
    """
    Generate search paths where objects of given maintype/objtype/subtype may be found.

    maintype is a toplevel type like 'imps', 'hss', or 'smwbase'
    objtype is a toplevel specfic type like 'hss', 'worksheet', 'config', 'ansible'
    subtype is an objtype specific type, config sets, for example have subtypes
    of 'global' and 'cle'
    """
    if not system:
        system = config.system
    if not system:
        raise ValueError('system undefined')

    gentype_arr = []
    systype_arr = [system]
    if subtype:
        gentype_arr.append(subtype)
        systype_arr.append(subtype)
    gentype_arr.append(objtype)
    systype_arr.append(objtype)

    gentype = '_'.join(gentype_arr)
    systype = '_'.join(systype_arr)

    paths = []
    for repo in repos:
        rpath = getattr(config, repo, None)
        if not rpath:
            continue
        paths.append(os.path.join(rpath, maintype, gentype))
        paths.append(os.path.join(rpath, maintype, systype))

    return [x for x in paths if os.path.exists(x) and os.access(x, os.R_OK)]

def get_objects(config, maintype, objtype, subtype=None, extra_obj_parameters=None, repos=('smwconf', 'secured'), system=None):
    """
    Get a dictionary of objects, annorated with the most relevant manifest
    entry, if it exists.
    """
    if not extra_obj_parameters:
        extra_obj_parameters = {}
    if not system:
        system = config.system
    output = {}
    paths = gen_paths(config, maintype, objtype, subtype=subtype, repos=repos, system=system)
    for path in paths:
        manifest = smwflow.manifest.Manifest(path)
        rpath = os.path.realpath(path)
        start_idx = len(rpath) + 1

        for (dirpath, _, filenames) in os.walk(rpath):
            for filename in filenames:
                if filename == '.smwflow.manifest.yaml':
                    continue
                filename = os.path.join(dirpath, filename)[start_idx:]
                output[filename] = smwflow.SmwflowObject(name=filename, fullpath=os.path.join(rpath, filename))
                if filename in manifest:
                    for key in manifest[filename]:
                        output[filename][key] = manifest[filename][key]
                if filename in extra_obj_parameters:
                    for key in extra_obj_parameters[filename]:
                        output[filename][key] = extra_obj_parameters[filename][key]
    return output
