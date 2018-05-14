import os
import sys
import difflib
import yaml
import json
import ConfigParser
import io

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
   return raw


def __diff_basic_tree(git_data, smw_data, typestr, keyskiplist):
    ret = []
    if type(git_data) is not type(smw_data):
        return ['git type (%s) != smw type (%s) for %s' % (str(type(git_data)), str(type(smw_data)), typestr)]

    if type(git_data) is dict:
    	smwKeys = set(smw_data.keys())
        gitKeys = set(git_data.keys())

        missingInGit = smwKeys.difference(gitKeys)
        missingInSystem = gitKeys.difference(smwKeys)
        common = smwKeys.intersection(gitKeys)

        for item in missingInGit:
            ret.append("smw %s:%s" % (typestr, item))
        for item in missingInSystem:
            ret.append("git %s:%s" % (typestr, item))

        for item in common:
            if item in keyskiplist:
                continue
            ret.extend(__diff_basic_tree(git_data[item], smw_data[item], "%s:%s" % (typestr, item), keyskiplist))
    elif type(git_data) is list:
        missingInGit = [x for x in git_data if x not in smw_data]
        missingInSmw = [x for x in smw_data if x not in git_data]
        common = [x for x in smw_data if x in git_data]

        for item in missingInGit:
            ret.append("smw %s:%s" % (typestr, item))
        for item in missingInSmw:
            ret.append("git %s:%s" % (typestr, item))

        for item in common:
            git_idx = git_data.index(item)
            smw_idx = smw_data.index(item)
            ret.extend(__diff_basic_tree(git_data[git_idx], smw_data[smw_idx], "%s:%s" % (typestr, item), keyskiplist))
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

def __diff_yaml(config, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_yaml = yaml.load(git_data)
    smw_yaml = yaml.load(smw_data)
    return __diff_basic_tree(git_yaml, smw_yaml, obj_data['name'], ignore_keys)

def __diff_json(config, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_json = json.loads(git_data)
    smw_json = json.loads(smw_data)
    return __diff_basic_tree(git_json, smw_json, obj_data['name'], ignore_keys)

def __parse_ini(input_str):
    ret = {}
    ip = io.StringIO(input_str)
    parser = ConfigParser.RawConfigParser(allow_no_value=True)
    parser.readfp(ip)
    ip.close()

    for sec in parser.sections():
        ret[sec] = {}
        for key,value in parser.items(sec):
            ret[sec][key] = value
    return ret

def __diff_ini(config, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_ini = __parse_ini(git_data)
    smw_ini = __parse_ini(smw_data)
    return __diff_basic_tree(git_ini, smw_ini, obj_data['name'], ignore_keys)

def __parse_keyvalue(input_str):
    ret = {}
    ip = io.StringIO(input_str)
    linenum = 0
    for line in ip:
        line = line.strip()
        linenum += 1
        if len(line) == 0 or line.startswith('#'):
            continue
        eq_pos = line.find('=')
        if eq_pos < 0:
            raise ValueError('Invalid keyvalue file, no equal sign on line %d: %s' % (linenum, line))
        key,value = line.split('=', 1)
        key = key.strip()
        value = value.strip()
        ret[key] = value
    ip.close()
    return ret

def __parse_keyspacevalue(input_str):
    ret = {}
    ip = io.StringIO(input_str)
    linenum = 0
    for line in ip:
        line = line.strip()
        linenum += 1
        if len(line) == 0 or line.startswith('#'):
            continue
        key,value = line.split(None, 1)
        key = key.strip()
        value = value.strip()
        ret[key] = value
    ip.close()
    return ret

def __diff_keyvalue(config, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_kv = __parse_keyvalue(git_data)
    smw_kv = __parse_keyvalue(smw_data)
    return __diff_basic_tree(git_kv, smw_kv, obj_data['name'], ignore_keys)

def __diff_keyspacevalue(config, obj_data, git_data, smw_data):
    ignore_keys = obj_data['ignore_keys'] if 'ignore_keys' in obj_data else []
    git_kv = __parse_keyspacevalue(git_data)
    smw_kv = __parse_keyspacevalue(smw_data)
    return __diff_basic_tree(git_kv, smw_kv, obj_data['name'], ignore_keys)

def basic_compare(config, obj_data, git_data, smw_data):
    filetype = guess_type(config, obj_data)

    if filetype == 'raw':
        diff = difflib.Differ()
        res = list(diff.compare(git_data, smw_data))
        final = []
        for line in res:
            if line[0] == '-':
                final.append('git: %s' % line[1:])
            elif line[0] == '+':
                final.append('smw: %s' % line[1:])
        return final
    elif filetype == 'ansiblevault':
        return __diff_ansiblevault(config, obj_data, git_data, smw_data)
    elif filetype == 'yaml':
        return __diff_yaml(config, obj_data, git_data, smw_data)
    elif filetype == 'json':
        return __diff_json(config, obj_data, git_data, smw_data)
    elif filetype == 'ini':
        return __diff_ini(config, obj_data, git_data, smw_data)
    elif filetype == 'keyvalue':
        return __diff_keyvalue(config, obj_data, git_data, smw_data)
    elif filetype == 'keyspacevalue':
        return __diff_keyspacevalue(config, obj_data, git_data, smw_data)
    raise ValueError('Unknown filetype in smwflow.compare.basic_compare: %s' % filetype)
