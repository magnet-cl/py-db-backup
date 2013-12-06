import os
from boto.s3.connection import S3Connection


class AmazonS3(object):
    def __init__(self, access_key, secret_key, bucket):
        """ Initialize the connection to amazon through the given access key.
        Then,  open a specific bucket.

        """
        # amazon s3 connection
        self.s3_conn = None
        self.__set_amazon_s3_service__(access_key, secret_key)

        # data bucket to be used
        self.bucket = self.s3_conn.get_bucket(bucket)

    def __set_amazon_s3_service__(self, access_key, secret_key):
        """ Establish the connection with amazon. """
        self.s3_conn = S3Connection(access_key, secret_key)

    def upload_file(self, key, filepath, access, keep_original=True,
                    verbose=False):
        """ Uploads a file to Amazon S3

        Arguments:
            key -- unique key
            filepath -- absolute path to file
            access -- 'private', 'public-read',
                      'public-read-write', 'authenticated-read'
            keep_original -- flag to remove the original file

        """

        # file entry
        try:
            file_entry = self.bucket.new_key(key)
            file_entry.set_metadata('filepath', filepath)
            file_entry.set_contents_from_filename(filepath)
            file_entry.set_acl(access)  # access control
        except Exception as error:
            print str(error)
            return False
        else:
            if verbose:
                print "%s uploaded to amazon s3." % key

        # original file removal
        if not keep_original and os.access(filepath, os.W_OK):
            try:
                os.remove(filepath)
            except (IOError, OSError):
                print "I/O error, could not remove file."
            else:
                if verbose:
                    print "%s (original) removed" % filepath

        return True
