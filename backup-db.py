from ConfigParser import ConfigParser
from distutils.dir_util import mkpath
from distutils.errors import DistutilsFileError
import gzip
from os import remove
from sh import pg_dump
from time import gmtime, strftime

from amazon_s3 import AmazonS3


def parse_configuration():
    """ Parses config file. """
    config = ConfigParser()
    config.read('config.ini')

    return config


def generate_backup():
    """ Generates the database dump. """

    print "Generating database dump."

    # configuration file
    config = parse_configuration()

    # dumps folder creation
    dumps_folder = "db_dumps"
    try:
        mkpath(dumps_folder)
    except DistutilsFileError:
        print "Error: can't mkdir {}".format(dumps_folder)
        return False

    # dump name generation
    dump_name = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    dump_name = "{}/{}.sql".format(dumps_folder, dump_name)

    # db engine
    db_engine = config.get('db', 'engine')

    if db_engine == 'postgresql':
        pg_dump(config.get('db', 'name'), '-f', dump_name)
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


def upload_backup():
    """ Uploads the generated dump to Amazon S3. """

    # configuration file
    config = parse_configuration()

    # amazon connection
    s3 = AmazonS3(
        config.get('aws', 'access_key'),
        config.get('aws', 'secret_key'),
        config.get('aws', 'bucket_name')
    )

    # dump generation
    key, filepath = generate_backup()

    if s3.upload_file(key, filepath, access='private'):
        print "Successfully uploaded {}".format(key)
    else:
        print "An error has ocurred while uploading {}".format(key)


if __name__ == '__main__':
    upload_backup()
