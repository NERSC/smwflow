import os
import sys
import smwflow.manifest

def gen_paths(config, maintype, objtype, subtype=None, repos=('smwconf','secured')):
    """
    Generate search paths where objects of given maintype/objtype/subtype may be found.

    maintype is a toplevel type like 'imps', 'hss', or 'smwbase'
    objtype is a toplevel specfic type like 'hss', 'worksheet', 'config', 'ansible'
    subtype is an objtype specific type, config sets, for example have subtypes
    of 'global' and 'cle'
    """
    system = config.system

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

def get_objects(config, maintype, objtype, subtype=None, repos=('smwconf','secured')):
    """
    Get a dictionary of objects, annorated with the most relevant manifest
    entry, if it exists.
    """
    output = {}
    paths = gen_paths(config, maintype, objtype, subtype, repos)
    for path in paths:
        manifest = smwflow.manifest.Manifest(path)

        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if filename == '.smwflow.manifest.yaml':
                    continue
                output[filename] = { 'fullpath': os.path.join(dirpath, filename) }
                if filename in manifest:
                    for key in manifest[filename]:
                        output[filename][key] = manifest[filename][key]
        
    return output
