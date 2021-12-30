
"""

Description:

- This module sets basic configs

"""

import os
# import getpass
import inspect
import logging

# TODO: Could we simply use the logging codes instead of maintaining this map?
log_levels = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warn': logging.WARN,
              'error': logging.ERROR}

if os.environ.get('MLTEMPLATE_LOG_PATH'):
    LOG_PATH = os.environ.get('MLTEMPLATE_LOG_PATH')
elif os.path.exists(r'P:\Proj\public\production_engineering\data_science\mltemplate\log'):
    LOG_PATH = r'P:\Proj\public\production_engineering\data_science\mltemplate\log'
elif os.path.exists(r'/proj/public/production_engineering/data_science/mltemplate/log'):
    LOG_PATH = r'/proj/public/production_engineering/data_science/mltemplate/log'
else:
    LOG_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def init_logger(name, log_level='warn'):
    """
    Init logger
    :param name:        logger name
    :param log_level:   Set the console log level;
                        Options are debug, info, warn, error
    :return:            logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_levels['debug'])
    formatter = logging.Formatter('%(asctime)s, %(name)s, %(levelname)s, %(message)s')

    # logger.handlers = []
    for hndlr in logger.handlers:
        hndlr.setFormatter(formatter)

    # formatter = logging.Formatter('%(message)s')
    # ch = logging.StreamHandler()
    # ch.setLevel(log_levels[log_level])
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    # secretly log to LOG_PATH
    # NOTE: was handlers[0] below

    # formatter = logging.Formatter('%(asctime)s, %(name)s, %(levelname)s, %(message)s')
    # fh = logging.FileHandler(os.path.join(LOG_PATH, '{}.csv'.format(getpass.getuser())), mode='a')
    # fh.setLevel(log_levels['info'])
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

    return logger


def config_logger(name,
                  clvl='warn',
                  flvl='debug',
                  file_path=None,
                  file_mode='w',
                  log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    """
    Set logger level and format
    Always create a console stream handler
    If file_path is not None, create FileHandler
    :param name:        str name of logger
    :param clvl:        str console log level
    :param flvl:        str file log level
    :param file_path:   str filepath
    :param file_mode:   str file mode; standard file handle open mode options
    :param log_format:  str log format
    """
    logger = logging.getLogger(name)
    formatter = logging.Formatter(log_format)

    # NOTE: Leaving this try, except for backwards compatibility with
    #       older usages of init_logger.
    try:
        logger.handlers[0].setFormatter(formatter)
        logger.handlers[0].setLevel(log_levels[clvl])
    except IndexError:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(log_levels[clvl])
        logger.addHandler(sh)

    if file_path is not None:
        fh = logging.FileHandler(os.path.join(file_path), mode=file_mode)
        fh.setLevel(log_levels[flvl])
        fh.setFormatter(formatter)
        logger.addHandler(fh)


def log_fun_info(logger, log_arg=False):
    """
    Log function calls
    :param logger:      logger to pass in.
    :param log_arg:     log function arguments or not.
    """
    frame, _, _, fn, _, _ = inspect.getouterframes(inspect.currentframe())[1]
    if not log_arg:
        logger.info('{}'.format(fn))
    else:
        _, _, _, argv = inspect.getargvalues(frame)
        argv.pop('self', None)
        argv = str(argv).replace(',', ';').replace('\n', '')
        logger.info('{}, {}'.format(fn, argv))


# Default logger
logger = init_logger(__name__)
