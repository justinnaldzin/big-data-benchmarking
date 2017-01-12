##########################################################################################
# Reads and parses the configuration file
##########################################################################################

import sys
import logging
from configparser import ConfigParser


def read_config(section, filename='config.ini'):
    logging.debug("function: parse_config.read_config()")
    logging.info("Parsing the configuration file")
    # create parser and read configuration file
    parser = ConfigParser()
    parser.read(filename)
    # get section
    dictionary = {}
    if parser.has_section(section):
        logging.info('[{0}] section read from {1} file'.format(section, filename))
        items = parser.items(section)
        for item in items:
            dictionary[item[0]] = item[1]
    else:
        logging.error('[{0}] section not found in the {1} file'.format(section, filename))
        logging.info('Exiting')
        sys.exit(1)
    return dictionary
