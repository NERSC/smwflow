#!/usr/bin/python

import sys
import smwflow.config
import smwflow.process

def main(argv):
    base_config = smwflow.config.BaseConfig()
    config = smwflow.config.ArgConfig(base_config, argv).values
    rc = smwflow.process.process(config)
    sys.exit(rc)

if __name__ == "__main__":
    main(sys.argv[1:])
