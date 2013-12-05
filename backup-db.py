from amazon_s3 import AmazonS3
from ConfigParser import ConfigParser


def parse_configuration():
    """ Parses config file. """
    config = ConfigParser()
    config.read('config.ini')

    return config


def generate_backup():
    """ Generates the database dump. """
    dump_name = ""
    filepath = ""

    return dump_name, filepath


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
