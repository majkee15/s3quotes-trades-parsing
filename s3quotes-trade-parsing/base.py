# __init__.py

import logging
import os
from pathlib import Path

log_format = "%(asctime)s::%(levelname)s::%(name)s::"\
             "%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(level='INFO', format=log_format, handlers=[logging.FileHandler("s3quotes_log.log", mode='w'),
                              logging.StreamHandler()])


class BaseMixin:

    def __init__(self, download_path=None):
        self.dataset_name = 'crypto-sobcap-data'

        if download_path is None:
            self.download_path = str(Path.home())
        else:
            self.download_path = download_path

        self.download_path = os.path.join(self.download_path, self.dataset_name)
        # Logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Instantiated class {self.__class__.__name__} with a download path '{self.download_path}'")

    def build_folder(self, rewrite=False):
        os.makedirs(self.download_path, exist_ok=rewrite)


if __name__ == '__main__':
    BaseMixin()