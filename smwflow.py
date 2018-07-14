#!/usr/bin/python

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
