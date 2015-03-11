# -*- coding: utf-8 -*-

# sh
from sh import pg_dump

# standard library
from ConfigParser import ConfigParser
from ConfigParser import NoSectionError
from distutils.dir_util import mkpath
from distutils.errors import DistutilsFileError
from os import remove
from os.path import expanduser
from time import gmtime
from time import strftime
import argparse
import gzip

from amazon_s3 import AmazonS3


def generate_backup(config):
    """ Generates the database dump. """

    print "Generating database dump."

    # dumps folder creation
    dumps_folder = "{}/cron_db_dumps".format(expanduser('~'))
    try:
        mkpath(dumps_folder)
    except DistutilsFileError:
        print "Error: can't mkdir {}".format(dumps_folder)
        return False

    # dump name generation
    dump_name = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    dump_name = "{}/{}.dump".format(dumps_folder, dump_name)

    # db engine
    db_engine = config.get('db', 'engine')

    if db_engine == 'postgresql':
        dump_format = '-Fc'  # custom format
        pg_dump(dump_format, config.get('db', 'name'), '-f', dump_name)
    else:
        print "Error: DB engine not supported."
        return False

    # gzip dump
    print "Compressing database dump."
    with open(dump_name, 'rb') as dump_file:
        dump_gzipped = gzip.open("{}.gz".format(dump_name), 'wb')
        dump_gzipped.writelines(dump_file)
        dump_gzipped.close()

    # remove raw file
    remove(dump_name)

    return "{}.gz".format(dump_name)


def upload_backup(config):
    """ Uploads the generated dump to Amazon S3. """

    # dump generation
    dump_name = generate_backup()

    if dump_name:
        # amazon connection
        print "Connecting to Amazon S3."
        s3 = AmazonS3(
            config.get('aws', 'access_key'),
            config.get('aws', 'secret_key'),
            config.get('aws', 'bucket_name')
        )

        print "Uploading compressed database dump."
        if s3.upload_file(dump_name, dump_name, access='private'):
            print "Successfully uploaded {}".format(dump_name)
        else:
            print "An error has ocurred while uploading {}".format(dump_name)
    else:
        print "An error has ocurred while dumping {}".format(dump_name)


def backup_handler(config_file):

    # parses config file
    config = ConfigParser()
    config.read(config_file)

    # check if the amazon credentials are set
    try:
        credentials_set = (
            config.get('aws', 'access_key') != 'key' and
            config.get('aws', 'secret_key') != 'secret'
        )
    except NoSectionError:
        print "Invalid configuration file"
        return False

    if credentials_set:
        upload_backup(config)
    else:
        generate_backup(config)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Backup database defined in configuration file'
    )
    parser.add_argument('-c', dest='config_file', default='config.ini',
                        help='configuration file')

    args = parser.parse_args()

    backup_handler(args.config_file)
