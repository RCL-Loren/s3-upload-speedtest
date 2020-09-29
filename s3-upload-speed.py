import os
import sys
import time
import boto3
import click
import logging
import threading

from dotenv import load_dotenv
from botocore.config import Config
from botocore.exceptions import ClientError

units_dict = {
    'k':1024,
    'm':1048576,
    'g':1073741824
}

load_dotenv()
AWS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET = os.getenv("AWS_SECRET")


@click.command()
@click.option('--size', default='1k', help='Size of file upload in bytes where \
the size is an integer followed by a single letter indicating unit k=kilo, \
m=mega, g=giga. Defaults to 1k')

@click.option('--iter', default=1, help='Number of upload iterations. Defaults \
to 1. To ensure no caching, it will generate a file for each iteration to \
prevent any caching from impacting the metrics. There needs to be enough \
disk space for the files, and beware that large numbers of large files will \
take time to generate')

@click.option('--bucket', default='p2vptpm-lcharnley-cenpro-12736', help='Name of Amazon S3 Bucket \
name for upload test')

@click.option('--loglevel', default='INFO', help='DEBUG, INFO, WARNING, ERROR, CRITICAL')

def run_test(size, iter, bucket, loglevel):
    log_config(logging, loglevel)

    file_size = get_size(size)[0]*units_dict[get_size(size)[1]]

    logging.info("Start Time: %s", time.perf_counter())
    logging.info("Files size parameter: %s", file_size)
    logging.info("Iterations parameter: %s", str(iter))
    logging.info('S3 Bucket: %s', bucket)
    
    #Generate Files
    print("Generating " + str(iter) + " Test Files")
    x = range(0, iter)
    for n in x:
        new_file_name = 'test_' + str(n)
        generate_big_random_bin_file(new_file_name, file_size)

    #Test Transfers
    for n in x:
        new_file_name = 'test_' + str(n)
        #Standard S3
        #Transfer
        upload_s3(new_file_name, bucket)



        #Acclerated S3
        #Transfer

        #Delete both non-accelerated and accelerated file from bucket.


    #Delete Files
    for n in x:
        new_file_name = 'test_' + str(n)
        print("Removing " + new_file_name)
        os.remove(new_file_name)

    logging.info("End Time: %s", time.perf_counter())

def upload_s3(filename, s3_bucket, use_accel=False):
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, config=Config(s3={'use_accelerate_endpoint': False}))
    try:
        start_time = time.perf_counter()
        response = s3.upload_file(filename, s3_bucket, filename, Callback=ProgressPercentage(filename))
        end_time = time.perf_counter()
    except ClientError as e:
        logging.error(e)

    print(": Upload time(s): " + str(end_time-start_time))

def gen_files():
    logging.info("Generating Files")

def get_size(file_size):
    unit = file_size[-1]
    significand = int(file_size[:-1])
    return [significand, unit]

def log_config(logger, log_level):
    numeric_level = getattr(logger, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logger.basicConfig(level=numeric_level)

def generate_big_random_bin_file(filename,size):
    """
    generate big binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :return:void
    """
    import os 
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size)) #1

    print('big random binary file with size %f generated ok'%size)
    pass

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()



if __name__ == '__main__':
    run_test()