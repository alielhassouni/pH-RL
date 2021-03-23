"""
@author: Ali el Hassouni
"""

import logging

base_logger = logging.getLogger("MoodBuster:root")
base_log_level = logging.DEBUG
formatter = logging.Formatter('%(asctime) s [%(name)s][%(levelname)s] - %(message)s')


def get_logger(logger_name: str, log_level=logging.DEBUG):
    """
    Return logger object.
    :param logger_name: The name of the base logger.
    :param log_level: Logging level.
    :return: Logging object.
    """
    logger = logging.getLogger("MoodBuster:{}".format(logger_name))
    logger.setLevel(log_level)
    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    return logger

