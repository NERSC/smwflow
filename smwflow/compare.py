import difflib
import json
import ConfigParser
import io
import yaml

def guess_type(config, obj):
    if 'formattype' in obj:
        return obj['formattype']
    if obj['name'] == ('%s_secrets.yaml' % config.system):
        return 'ansiblevault'
    if obj['name'].endswith('_secrets.yaml'):
        return 'raw'
    if obj['name'].endswith('.yaml'):
        return 'yaml'
    if obj['name'].endswith('.json'):
        return 'json'
    if obj['name'].endswith('.ini'):
        return 'ini'
    return 'raw'


def __diff_basic_tree(git_data, smw_data, typestr, keyskiplist):
    ret = []
    if type(git_data) is not type(smw_data):
        return ['git type (%s) != smw type (%s) for %s' % \
               (str(type(git_data)), str(type(smw_data)), typestr)]

    if isinstance(git_data, dict):
        smw_keys = set(smw_data.keys())
        git_keys = set(git_data.keys())

        missing_in_git = smw_keys.difference(git_keys)
        missing_in_system = git_keys.difference(smw_keys)
        common = smw_keys.intersection(git_keys)

        for item in missing_in_git:
            ret.append("smw %s:%s" % (typestr, item))
        for item in missing_in_system:
            ret.append("git %s:%s" % (typestr, item))

        for item in common:
            if item in keyskiplist:
                continue
            ret.extend(__diff_basic_tree(git_data[item], smw_data[item],
                                         "%s:%s" % (typestr, item), keyskiplist))
    elif isinstance(git_data, list):
        missing_in_git = [x for x in git_data if x not in smw_data]
        missing_in_smw = [x for x in smw_data if x not in git_data]
        common = [x for x in smw_data if x in git_data]

        for item in missing_in_git:
            ret.append("smw %s:%s" % (typestr, item))
        for item in missing_in_smw:
            ret.append("git %s:%s" % (typestr, item))

        for item in common:
            git_idx = git_data.index(item)
            smw_idx = smw_data.index(item)
            ret.extend(__diff_basic_tree(git_data[git_idx], smw_data[smw_idx],
                                         "%s:%s" % (typestr, item), keyskiplist))
    elif git_data != smw_data:
        ret.append("smw %s:%s" % (typestr, smw_data))
        ret.append("git %s:%s" % (typestr, git_data))
    return ret

def __diff_ansiblevault(config, obj_data, git_data, smw_data):
    try:
        ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
        git_yaml = yaml.load(config.vaultobj.decrypt(git_data))
        smw_yaml = yaml.load(config.vaultobj.decrypt(smw_data))
        return __diff_basic_tree(git_yaml, smw_yaml, obj_data['name'], ignore_keys)
    except:
        pass
    return []

def __diff_yaml(_, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_yaml = yaml.load(git_data)
    smw_yaml = yaml.load(smw_data)
    return __diff_basic_tree(git_yaml, smw_yaml, obj_data['name'], ignore_keys)

def __diff_json(_, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_json = json.loads(git_data)
    smw_json = json.loads(smw_data)
    return __diff_basic_tree(git_json, smw_json, obj_data['name'], ignore_keys)

def __parse_ini(input_str):
    ret = {}
    input_fp = io.StringIO(input_str)
    parser = ConfigParser.RawConfigParser(allow_no_value=True)
    parser.readfp(input_fp)
    input_fp.close()

    for sec in parser.sections():
        ret[sec] = {}
        for key, value in parser.items(sec):
            ret[sec][key] = value
    return ret

def __diff_ini(_, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_ini = __parse_ini(git_data)
    smw_ini = __parse_ini(smw_data)
    return __diff_basic_tree(git_ini, smw_ini, obj_data['name'], ignore_keys)

def __parse_keyvalue(input_str):
    ret = {}
    input_fp = io.StringIO(input_str)
    linenum = 0
    for line in input_fp:
        line = line.strip()
        linenum += 1
        if not line or line.startswith('#'):
            continue
        eq_pos = line.find('=')
        if eq_pos < 0:
            raise ValueError('Invalid keyvalue file, no equal sign on line %d: %s' % \
                             (linenum, line))
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip()
        ret[key] = value
    input_fp.close()
    return ret

def __parse_keyspacevalue(input_str):
    ret = {}
    input_fp = io.StringIO(input_str)
    linenum = 0
    for line in input_fp:
        line = line.strip()
        linenum += 1
        if not line or line.startswith('#'):
            continue
        key, value = line.split(None, 1)
        key = key.strip()
        value = value.strip()
        ret[key] = value
    input_fp.close()
    return ret

def __diff_keyvalue(_, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_kv = __parse_keyvalue(git_data)
    smw_kv = __parse_keyvalue(smw_data)
    return __diff_basic_tree(git_kv, smw_kv, obj_data['name'], ignore_keys)

def __diff_keyspacevalue(_, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_kv = __parse_keyspacevalue(git_data)
    smw_kv = __parse_keyspacevalue(smw_data)
    return __diff_basic_tree(git_kv, smw_kv, obj_data['name'], ignore_keys)

def basic_compare(config, obj_data, git_data, smw_data):
    filetype = guess_type(config, obj_data)
    ret = None
    if filetype == 'raw':
        l_git_data = git_data.split()
        l_smw_data = smw_data.split()
        diff = difflib.Differ()
        res = list(diff.compare(l_git_data, l_smw_data))
        final = []
        for line in res:
            if line[0] == '-':
                final.append('git: %s' % line[1:])
            elif line[0] == '+':
                final.append('smw: %s' % line[1:])
        ret = final
    elif filetype == 'ansiblevault':
        ret = __diff_ansiblevault(config, obj_data, git_data, smw_data)
    elif filetype == 'yaml':
        ret = __diff_yaml(config, obj_data, git_data, smw_data)
    elif filetype == 'json':
        ret = __diff_json(config, obj_data, git_data, smw_data)
    elif filetype == 'ini':
        ret = __diff_ini(config, obj_data, git_data, smw_data)
    elif filetype == 'keyvalue':
        ret = __diff_keyvalue(config, obj_data, git_data, smw_data)
    elif filetype == 'keyspacevalue':
        ret = __diff_keyspacevalue(config, obj_data, git_data, smw_data)
    else:
        raise ValueError('Unknown filetype in smwflow.compare.basic_compare: %s' % filetype)
    return ret
