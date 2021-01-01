import logging

from downloader import Downloader

# package logger
package_logger = logging.getLogger(__name__)
package_logger.setLevel(logging.DEBUG)

# not needed for this small project
# # defines the stream handler
# _ch = logging.StreamHandler()  # creates the handler
# _ch.setLevel(logging.INFO)  # sets the handler info
# _ch.setFormatter(logging.Formatter(INFOFORMATTER))  # sets the handler formatting
#
# # adds the handler to the global variable: log
# log.addHandler(_ch)

__all__ = ['package_logger', 'Downloader']