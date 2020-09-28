import os
import time
import click
import logging

units_dict = {
    'k':1024,
    'm':1048576,
    'g':1073741824
}


@click.command()
@click.option('--size', default='1k', help='Size of file upload in bytes where \
the size is an integer followed by a single letter indicating unit k=kilo, \
m=mega, g=giga. Defaults to 1k')

@click.option('--iter', default=1, help='Number of upload iterations. Defaults \
to 1. To ensure no caching, it will generate a file for each iteration to \
prevent any caching from impacting the metrics. There needs to be enough \
disk space for the files, and beware that large numbers of large files will \
take time to generate')

@click.option('--bucket', default='MyBucket', help='Name of Amazon S3 Bucket \
name for upload test')

@click.option('--loglevel', default='DEBUG', help='DEBUG, INFO, WARNING, ERROR, CRITICAL')

def run_test(size, iter, bucket, loglevel):
    log_config(logging, loglevel)

    file_size = get_size(size)[0]*units_dict[get_size(size)[1]]

    logging.debug("Start Time: %s", time.perf_counter())
    logging.debug("Files size parameter: %s", file_size)
    logging.debug("Iterations parameter: %s", str(iter))
    logging.debug('S3 Bucket: %s', bucket)
    


    #Generate Files
    generate_big_random_bin_file('test', file_size)



    logging.debug("End Time: %s", time.perf_counter())

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





if __name__ == '__main__':
    run_test()