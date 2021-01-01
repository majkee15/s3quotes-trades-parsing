# loader.py

import numpy as np
import pandas as pd

from base import BaseMixin

class Loader(BaseMixin):
    def __init__(self, path):
        super().__init__(path)

    def load_symbol(self, symbol, date):
        pass


