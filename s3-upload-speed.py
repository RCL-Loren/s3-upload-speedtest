import click
import logging

def gen_files():
    logging.info("Generating Files")

def get_size(file_size):
    unit = file_size[-1]
    significand = int(file_size[:-1])
    return [significand, unit]

@click.command()
@click.option('--size', default='1k', help='Size of file upload in bytes where \
the size is an integer followed by a single letter indicating unit k=kilo, \
m=mega, g=giga. Defaults to 1k')
@click.option('--iter', default=1, help='Number of upload iterations. Defaults \
to 1. To ensure no caching, it will generate a file for each iteration to \
prevent any caching from impacting the metrics. There needs to be enough \
disk space for the files, and beware that large numbers of large files will \
take time to generate')
@click.option('--loglevel', default='INFO', help='DEBUG, INFO, WARNING, ERROR, CRITICAL')
def run_test(size, iter, loglevel):
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level)

    logging.debug('Default Logging Level INFO')
    print(get_size(size))

if __name__ == '__main__':
    run_test()


