"""
"""
import os
import subprocess
import re
import smwflow.hss as hss
import smwflow.imps as imps
import smwflow.cfgset as cfgset

def get_git_head_rev(path):
    command = ["git", "-C", path, "rev-parse", "HEAD"]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    return stdout.strip()

def get_git_branch(path):
    command = ["git", "-C", path, "status", "-s", "--porcelain", "-b", "-u", "no"]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    stdout = stdout.strip()
    loc = stdout.find('...')
    if loc >= 0:
        stdout = stdout[:loc]

    match = re.match(r'##\s+(.*)(\.\.\.(.*))?', stdout.strip())
    if match:
        return match.groups()[0]
    return None

def _checkout_git_branch(path, branch, do_pull=False):
    have_remote = False
    command = ['git', '-C', path, 'remote']
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    if stdout.strip():
        have_remote = True
    if do_pull and have_remote:
        command = ['git', '-C', path, 'fetch']
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, _ = proc.communicate()
        if proc.returncode != 0:
            raise "Failed git-fetch in %s" % path

    command = ['git', '-C', path, 'checkout', branch]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    if proc.returncode != 0:
        raise "Failed to checkout branch %s in %s" % (branch, path)

    if do_pull and have_remote:
        command = ['git', '-C', path, 'pull']
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, _ = proc.communicate()
        if proc.returncode != 0:
            raise "Failed to perform git-pull in %s" % path

    return proc.returncode

def do_status(config):
    smwconf_branch = "Unknown (inaccessible)"
    smwconf_head = "Unknown (inaccessible)"
    if os.access(config.smwconf, os.R_OK):
        smwconf_branch = get_git_branch(config.smwconf)
        smwconf_head = get_git_head_rev(config.smwconf)
    secured_branch = "Unknown (inaccessible)"
    secured_head = "Unknown (inaccessible)"
    if os.access(config.secured, os.R_OK):
        secured_branch = get_git_branch(config.secured)
        secured_head = get_git_head_rev(config.secured)
    zypper_branch = "Unknown (inaccessible)"
    zypper_head = "Unknown (inaccessible)"
    if os.access(config.zypper, os.R_OK):
        zypper_branch = get_git_branch(config.zypper)
        zypper_head = get_git_head_rev(config.zypper)
    print "smwconf repo  : %s" % config.smwconf
    print "smwconf branch: %s" % smwconf_branch
    print "smwconf HEAD  : %s" % smwconf_head
    print "secured repo  : %s" % config.secured
    print "secured branch: %s" % secured_branch
    print "secured HEAD  : %s" % secured_head
    print "zypper repo   : %s" % config.zypper
    print "zypper branch : %s" % zypper_branch
    print "zypper HEAD   : %s" % zypper_head
    return []

def do_checkout(config):
    retcode = 0
    if os.access(config.smwconf, os.R_OK):
        print "\nStaring checkout on smwconf repo:"
        retcode += _checkout_git_branch(config.smwconf, config.smwconf_branch, config.checkout_pull)
    if os.access(config.secured, os.R_OK):
        print "\nStaring checkout on secured repo:"
        retcode += _checkout_git_branch(config.secured, config.secured_branch, config.checkout_pull)
    if os.access(config.zypper, os.R_OK):
        print "\nStaring checkout on zypper repo:"
        retcode += _checkout_git_branch(config.zypper, config.zypper_branch, config.checkout_pull)
    return []


def do_import(config):
    deferred_actions = []
    if config.import_hss:
        deferred_actions.extend(hss.import_data(config))
    if config.import_imps:
        deferred_actions.extend(imps.import_data(config))

    return deferred_actions

def do_verify(config):
    deferred_actions = []
    if config.verify_hss:
        deferred_actions.extend(hss.verify_data(config))
    if config.verify_imps:
        deferred_actions.extend(imps.verify_data(config))
    if config.verify_cfgset:
        deferred_actions.extend(cfgset.verify_data(config))

    return deferred_actions

def do_update(config):
    deferred_actions = []
    if config.update_hss:
        deferred_actions.extend(hss.update_data(config))
    if config.update_imps:
        deferred_actions.extend(imps.update_data(config))
    if config.update_cfgset:
        deferred_actions.extend(cfgset.update_data(config))
    return deferred_actions

def do_create(config):
    deferred_actions = []
    if config.create_cfgset:
        deferred_actions.extend(cfgset.create(config))
    return deferred_actions

def process(config):
    ret = None
    if config.mode == "status":
        ret = do_status(config)
    elif config.mode == "checkout":
        ret = do_checkout(config)
    elif config.mode == "import":
        ret = do_import(config)
    elif config.mode == "verify":
        ret = do_verify(config)
    elif config.mode == "update":
        ret = do_update(config)
    elif config.mode == "create":
        ret = do_create(config)
    return ret
