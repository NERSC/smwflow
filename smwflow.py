#!/usr/bin/python

import sys
import smwflow.config
import smwflow.process

def main(argv):
    config = smwflow.config.Config()
    config = smwflow.config.ArgConfig(config, argv).values
    rc = smwflow.process.process(config)
    print config
    sys.exit(rc)

if __name__ == "__main__":
    main(sys.argv[1:])
