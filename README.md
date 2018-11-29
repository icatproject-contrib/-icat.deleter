# Datafile Deleter

Takes a file containing one file path per line and delete the corresponding DataFile entities from ICAT

## Usage
```
delete_datafiles.py [-h] -f <locations-file> -i <http[s]://host:port>
                           -u <username> -m <authentication-mechanism>
                           [-b <batch-size>] [-l <log-level>]

Takes a file containing one file path per line and delete the corresponding
DataFile entities from ICAT

optional arguments:
  -h, --help            show this help message and exit
  -f <locations-file>, --locations-file <locations-file>
                        A file containing files to delete from ICAT, one file
                        per line
  -i <http[s]://host:port>, --icat-host <http[s]://host:port>
                        The ICAT host and port eg. 'http://example.com:8080/'
  -u <username>, --user <username>
                        The username to use to connect to ICAT
  -m <authentication-mechanism>, --mechanism <authentication-mechanism>
                        The authentication mechanism to use to connect to ICAT
                        eg. simple|db|ldap
  -b <batch-size>, --batch-size <batch-size>
                        The number of files to delete in a single call to ICAT
  -l <log-level>, --log-level <log-level>
                        The logging level
```

~~## Build
Use pex to create a compressed, executable file (zip) containing the python script and its dependencies. Only requires python to be installed on the target machine.~~

~~pex . requests -c delete_datafiles.py -o delete_datafiles.pex -f dist~~
