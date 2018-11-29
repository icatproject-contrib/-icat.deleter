from distutils.core import setup
setup(name='delete_datafiles',
        version='0.1',
        description= "Takes a file containing one file path per line and deletes the corresponding DataFile entities from ICAT",
        url="https://github.com/icatproject-contrib/icat.deleter",
        author="Stuart Pullinger",
        author_email="stuart.pullinger@stfc.ac.uk",
        scripts=['delete_datafiles.py'],
        )
