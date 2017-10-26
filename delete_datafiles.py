#!/usr/bin/env python
"""Takes a file containing one file path per line and delete the corresponding
DataFile entities from ICAT"""

import requests
import sys
import logging
import argparse
import itertools
import getpass

def get_session(auth_mechanism, username, password, host):
    """Takes a username, password and authentication mechanism, logs into ICAT
    and returns a session ID"""

    # The ICAT Rest API does not accept json in the body of the HTTP request.
    # Instead it takes the form parameter 'json' with a string value - which is
    # the json-encoded data - eurrgh! The json-encoded data is sensitive to
    # order so we cannot pass a Python dictionary to the requests.post call as
    # Python dictionaries do not preserve order - eurrgh! So we construct a
    # string with the json data in the correct order - an OrderedDict may work
    # here - untested. (Also, dictionaries preserve order in Python 3.something)
    form_data = {'json': '{"plugin": "' + auth_mechanism +
            '", "credentials":[{"username":"' + username +
            '"}, {"password":"' + password + '"}]}'}
    session_url = host + "/icat/session"

    response = requests.post(session_url, data=form_data)

    if response.ok:
        return response.json()['sessionId']
    else:
        message = "Failed to get a session ID. Exiting.\n"
        message += "URL: %s\n" % str(response.url)
        message += "Status Code: %s\n" % str(response.status_code)
        message += "Response: %s\n" % response.text if response.text else 'Empty'
        raise RuntimeError(message)

def get_datafile_id(location, host, session):
    """Takes a file path location as a string and returns the corresponding
    DataFile ID from ICAT"""

    jpql_query = "SELECT df.id FROM Datafile df WHERE df.location = '%s'" % location
    jpql_url = host + "/icat/entityManager/"
    query_data = (('sessionId', session), ('query', jpql_query))

    response = requests.get(jpql_url, params=query_data)

    # If there are no results, ICAT still returns 200 OK and an empty response
    # Therefore, we cannot check the response code
    try:
        return response.json()[0]
    except IndexError as e:
        message = "Failed to get Datafile ID for: %s. Skipping.\n" % location
        message += "URL: %s\n" % str(response.url)
        message += "Status Code: %s\n" % str(response.status_code)
        message += "Response: %s\n" % response.text if response.text else 'Empty'
        message += "Json: %s\n" % response.json() if response.json() else 'Empty'
        logging.warning(message)
        return None

def delete_datafiles(datafiles, host, session):
    """Takes a list of DataFile IDs and deletes them from ICAT"""

    print "%d datafiles" % len(datafiles)

    # Construct a delete query string of the form:
    # '[ {"Datafile": {"id": 9933}}, {"Datafile": {"id": 3316}} ]'
    datafiles_as_json = (''.join( ( '{"Datafile": {"id": ', str(x), '}}') ) for x in
            datafiles)
    # TODO make this an iterator - it currently constructs a string
    datafiles_with_commas = ', '.join(datafiles_as_json)
    datafiles_as_json_list = ''.join(('[ ', datafiles_with_commas, ' ]'))
    delete_url = host + "/icat/entityManager/"

    response = requests.delete(delete_url, params= (('sessionId', session),
        ('entities', datafiles_as_json_list)))

    if response.ok:
        return
    else:
        message = "Failed to delete datafiles: %s. Skipping.\n" % str(datafiles)
        message += "URL: %s\n" % str(response.url)
        message += "Status Code: %s\n" % str(response.status_code)
        message += "Response: %s\n" % response.text if response.text else 'Empty'
        message += "Json: %s\n" % response.json() if response.json() else 'Empty'
        print message
        return

#async def?
def process_locations_file(locations_file, batch_size, host, session):
    """Takes an open file of Datafile locations, iterates over them, getting the
    ID of each then deleting them in batches"""

    # Maybe make some lookups into local vars for speed?
    # Use itertools to get an iterator over batch-size (200?) lines from the locations file
    # Maybe convert the iterator to a list (so file is read in chunks - may be
    # faster than lots of small calls) and iterate over values (async for?)
    # Get url and append ID to list (aiohttp?)
    # After batch loop (async gather?), call delete(Many) url

    locations = iter(locations_file)

    while True:
        # Create a batch of locations to delete - a list of strings taken from
        # the locations file with whitespace stripped off the beginning and end
        batch = [l.strip() for l in itertools.islice(locations, batch_size)]
        if len(batch) == 0: #ie. nothing returned from locations file
            break

        deletes = list()
        #async for?
        for location in batch:
            datafile_id = get_datafile_id(location, host, session)
            if datafile_id is not None:
                deletes.append(datafile_id)
            # will this work OK in an async context? Maybe zip the batch
            # iterator with an xrange of batch-size to create an index into a
            # initialised list(deletes), then put the datafile_id into the
            # correct position in the index

        if len(deletes) != 0:
            delete_datafiles(deletes, host, session)
        else:
            logging.warning("No files to delete from the batch")

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
                            default=10,
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
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=getattr(logging, args.log_level.upper()))

    # ask user for password
    password = getpass.getpass("Enter password for %s/%s: " % (args.mechanism, args.user))

    # try opening file of locations
    locations_file = open(args.locations_file, 'r')
    logging.info("Opened locations file at: %s", args.locations_file)

    # try creating a session - how long will the session last?
    session = get_session(args.mechanism, args.user, password, args.icat_host)
    logging.info("Got session ID: %s", session)

    # call process_location_file
    process_locations_file(locations_file, args.batch_size, args.icat_host, session)

if __name__ == "__main__":
    main()
