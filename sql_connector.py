## name        =  "config.py"
## version     =  "0.1"

## summary     =  "Supports connectivity to databases and APIs"
## url         =  "Link to the script on GitHub"

## description = """This module enables passing name of a section within an .ini 
##                to obtain the parameters and or credentials necessary for initiating 
##                a connection to either a database or API"""

## authors     =  [author("Justin Martin","justin.martin@cerner.com")]
## dependecies =  [dependency("None","None")]



import glob, os
from configparser import ConfigParser


def config(section,filename):
    # create a parser
    section=section
    parser = ConfigParser()
    # read config file
    #parser.read(filename)

    # get section, default to postgresql
    db = {}

    # validate ini file is available
    if  os.path.isfile(filename):
        # read config file
        parser.read(filename)
    else: 
        raise Exception('Error: Could not read file {0}'.format(filename))

    # validate section is in the ini file
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
