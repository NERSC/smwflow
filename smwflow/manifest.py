import os
import sys
import yaml
import copy

class Manifest(object):
    def __init__(self, path):
        self.__curr__ = {}
        self.__orig__ = {}
        self.path = path
        self.changes = 0
        self.read()

    def read(self):
        fname = os.path.join(self.path, '.smwflow.manifest.yaml')
        if not os.path.exists(fname):
            return
        with open(fname, 'r') as fp:
            self.__orig__ = yaml.load(fp.read())
        self.__curr__ = copy.deepcopy(self.__orig__)

    def save(self):
        if self.__curr__ == self.__orig__:
            print "manifest identical"
        else:
            print "manifest altered"
        fname = os.path.join(self.path, '.smwflow.manifest.yaml')
        with open(fname, 'w') as fp:
            fp.write(yaml.dump(self.__curr__))
        return []

    def __getitem__(self, key):
        return self.__curr__[key]

    def __setitem__(self, key, value):
        self.__curr__[key] = value

    def __delitem__(self, key):
        del self.__curr__[key]

    def __contains__(self, key):
        return key in self.__curr__

    def __len__(self):
        return len(self.__curr__)

    def __repr__(self):
        return "Manifest for %s: %s" % (self.path, repr(self.__curr__))

