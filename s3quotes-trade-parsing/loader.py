# loader.py

import numpy as np
import pandas as pd
import os

from base import BaseMixin

class Loader(BaseMixin):
    def __init__(self, path=None):
        self.exchange = 'bitmex'
        self.format_type = '.parquet'
        super().__init__(path)
        self.download_path = os.path.join(self.download_path, self.exchange)
        if not os.path.exists(self.download_path):
            raise FileNotFoundError(f'The download path {self.download_path} does not exist.')
        else:
            self.avail_symbols = [s for s in  os.listdir(self.download_path) if not  s.startswith('.')]

    def load_symbol(self, symbol, start_stamp, end_stamp):
        # Load from multiple files
        pass

    def load_symbol_day(self, symbol, date, style='trans'):
        if style not in ['trans', 'quotes']:
            raise ValueError(f"Not supported style {style}.")
        parq_path = os.path.join(self.download_path, symbol, style, date + self.format_type)
        df = pd.read_parquet(parq_path)
        if style == 'quotes':
            df['midprice'] = (df.ask + df.bid) / 2
        return df




if __name__ == '__main__':
    ldr = Loader()
    print(ldr.avail_symbols)
    print(ldr.load_symbol_day('ETHUSD', '2020-12-01', 'trans'))