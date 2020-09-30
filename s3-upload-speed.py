import os
import sys
import csv
import time
import boto3
import click
import logging
import threading
import statistics

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

@click.option('--csvfilename', default='', help='If this option is given a filename it will generate a CSV file')

def run_test(size, iter, bucket, loglevel, csvfilename):
    log_config(logging, loglevel)

    file_size = get_size(size)[0]*units_dict[get_size(size)[1]]

    logging.info("Start Time: %s", time.perf_counter())
    logging.info("Files size parameter: %s", file_size)
    logging.info("Iterations parameter: %s", str(iter))
    logging.info('S3 Bucket: %s', bucket)

    normal_upload_times = []
    accel_upload_times = []
    normal_upload_speed = []
    accel_upload_speed = []
    
    #Generate Files
    gen_files(file_size, iter)

    #Test Transfers
    x = range(0, iter)
    for n in x:
        new_file_name = 'test_' + str(n)

        #Standard S3
        upload_time = upload_s3(new_file_name, bucket)
        normal_upload_times.append(upload_time)
        mb = file_size / units_dict['m']
        normal_upload_speed.append((mb/upload_time))
        logging.info("Upload speed: %s MB/s", (mb/upload_time))

        #Acclerated S3
        upload_time = upload_s3(new_file_name, bucket, use_accel=True)
        accel_upload_times.append(upload_time)
        accel_upload_speed.append((mb/upload_time))
        logging.info("Upload speed: %s MB/s", (mb/upload_time))

    #Delete Files
    for n in x:
        new_file_name = 'test_' + str(n)
        print("Removing " + new_file_name)
        os.remove(new_file_name)

    logging.info("End Time: %s", time.perf_counter())


    summary(size,iter,normal_upload_times, accel_upload_times, normal_upload_speed, accel_upload_speed, csvfilename)

def summary(size, runs, norm_times, accel_times, norm_speed, accel_speed, csv_file):
    
    #Descriptive Statistics
    print('\nResults Summary')
    print('File size: {} bytes'.format(size))
    print('Number of Runs: {}'.format(runs))
    print('Average Time (s) (Normal): {}'.format(statistics.mean(norm_times)))
    print('Average Time (s) (Accelerated): {}'.format(statistics.mean(accel_times)))
    print('Average Speed (MB/s) (Normal): {}'.format(statistics.mean(norm_speed)))
    print('Average Speed (MB/s) (Accelerated): {}'.format(statistics.mean(accel_speed)))
    
    rows = zip(norm_times, accel_times, norm_speed, accel_speed)

    #Output times and speeds to CSV File
    if csv_file:
        with open(csv_file, 'w', newline='\n', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(('Normal', 'Acclerated', 'Normal Speed MB/s', 'Accel Speed MB/s'))
            for row in rows:
                csvwriter.writerow(row)
        csvfile.close()


def upload_s3(filename, s3_bucket, use_accel=False):
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, config=Config(s3={'use_accelerate_endpoint': use_accel}))
    
    try:
        start_time = time.perf_counter()
        response = s3.upload_file(filename, s3_bucket, filename, Callback=ProgressPercentage(filename))
        end_time = time.perf_counter()
        s3.delete_object(Bucket=s3_bucket, Key=filename)
    except ClientError as e:
        logging.error(e)

    if use_accel:
        accel_status = ' (accelerated)'
    else:
        accel_status = ' (normal)'

    print(": Upload time: " + str(end_time-start_time) + "s" + accel_status)

    return(end_time-start_time)

def gen_files(size, iter):
    logging.info("Generating Files")
    x = range(0, iter)
    for n in x:
        new_file_name = 'test_' + str(n)
        generate_random_bin_file(new_file_name, size)

def get_size(file_size):
    unit = file_size[-1].lower()
    significand = int(file_size[:-1])
    return [significand, unit]

def log_config(logger, log_level):
    numeric_level = getattr(logger, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logger.basicConfig(level=numeric_level)

def generate_random_bin_file(filename, size):
    """
    generate binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :return:void
    """
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size)) #1

    logging.info('Generated file %s with %f bytes', filename, size)

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