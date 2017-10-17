#!/usr/bin/env python
"""Takes a file containing one file path per line and delete the corresponding DataFile entities from ICAT"""

import requests #use requests directly or use python-icat?
import sys
import logging
import argparse

# create global vars: host, session

def get_session(auth_mechanism, username, password):
    """Takes a username, password and authentication mechanism, logs into ICAT and returns a session ID"""
    pass

def get_datafile_id(location):
    """Takes a file path location as a string and returns the corresponding DataFile ID from ICAT"""
    pass

def delete_datafiles(datafiles):
    """Takes a list of DataFile IDs and deletes them from ICAT"""
    pass

def process_location_file(location_file):
    """Takes an open file of Datafile locations, iterates over them, getting the ID of each then deleting them in batches"""
    # Maybe make some lookups into local vars for speed?
    # Use itertools to get an iterator over batch-size (200?) lines from the locations file
    # Maybe convert the iterator to a list (so file is read in chunks - may be
    # faster than lots of small calls) and iterate over values (async for?)
    # Get url and append ID to list (aiohttp?)
    # After batch loop (async gather?), call delete(Many) url
    pass

def main():
    # parse commandline options
    arg_parser = argparse.ArgumentParser(description="Takes a file containing one file path per line and delete the corresponding DataFile entities from ICAT")
    arg_parser.add_argument('-f',
                            '--locations-file',
                            required=True,
                            help="A file containing files to delete from ICAT, one file per line",
                            metavar="<locations-file>",
                            dest="locations_file")
    arg_parser.add_argument('-i',
                            '--icat-host',
                            required=True,
                            help="The ICAT host and port eg. 'http://example.com:8080/'",
                            metavar="<http[s]://host:port>",
                            dest="icat_host")
    arg_parser.add_argument('-u',
                            '--user',
                            required=True,
                            help="The username to use to connect to ICAT",
                            metavar="<username>",
                            dest="user")
    arg_parser.add_argument('-m',
                            '--mechanism',
                            required=True,
                            help="The authentication mechanism to use to connect to ICAT eg. simple|db|ldap",
                            metavar="<authentication-mechanism>",
                            dest="mechanism")
    arg_parser.add_argument('-b',
                            '--batch-size',
                            default=200,
                            help="The number of files to delete in a single call to ICAT",
                            metavar="<batch-size>",
                            dest="batch_size")
    arg_parser.add_argument('-l',
                            '--log-level',
                            default='WARNING',
                            choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                            help="The logging level",
                            metavar="<log-level>",
                            dest="log_level")
    args = arg_parser.parse_args()
    # set up logging - from commandline option
    logging.setLevel(logging[args.log_leve])
    # ask user for password
    # try opening file of locations
    # try creating a session - how long will the session last?
    # call process_location_file

if __name__=="__main__":
    main()
