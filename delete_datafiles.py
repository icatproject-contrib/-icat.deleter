#!/usr/bin/env python
"""Takes a file containing one file path per line and delete the corresponding DataFile entities from ICAT"""

import requests #use requests directly or use python-icat?
import sys
import logging

# create global vars: host, session

def get_session(mnemonic, username, password):
    """Takes a username, password and mnemonic, logs into ICAT and returns a session ID"""
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
    # set up logging
    # ask user for password
    # try opening file of locations
    # try creating a session - how long will the session last?
    # call process_location_file

if __name__=="__main__":
    main()
