import os
import sys
import copy
import stat
import smwflow.search

def get_plugins(config, maintype, objtype, subtype=None, repos=('smwconf', 'secured'), system=None):
    paths = smwflow.search.gen_paths(config, maintype, "%s_plugins" % objtype, subtype, repos, system)
    paths_rev = [x for x in paths.__reversed__()]
    old_sys_path = copy.deepcopy(sys.path)
    sys.path.extend(paths_rev)

    plugins = {}
    modules = {}
    for path in paths:
        for fname in os.listdir(path):
            if not fname.endswith(".py"):
                continue

            fpath = os.path.join(path, fname)
            stdata = os.stat(fpath)
            if not stat.S_ISREG(stdata.st_mode):
                continue

            module_name = fname[:-3]
            # in case there are duplicate modules names, do this in two passes
            # to avoid loading duplicate copies (owing to the behavior of the
            # loader)
            modules[module_name] = True
    for module in modules:
        __module__ = __import__(module, globals(), locals(), [], -1)
        pluginall = getattr(__module__, "__all__")
        for plugin in pluginall:
            plugins[plugin] = getattr(__module__, plugin)

    sys.path = old_sys_path
    return plugins
