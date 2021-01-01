# downloader.py

from base import BaseMixin
import pandas as pd
import os
import shutil
import boto3
from botocore.exceptions import ClientError

# This should probably be in constructor and take profile as an arg
os.getenv('AWS_PROFILE', 'dynamodbSobCap')
s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket('sobesice-capital-data')


class Downloader(BaseMixin):
    def __init__(self, path=None):
        super().__init__(path)
        self.symbols = None
        self.avail_dict = {}
        # multiple exchanges would go here
        self.download_path = os.path.join(self.download_path, 'bitmex')
        self.skipped = []

    def read_boto_obj_to_df(self, ticker, date, style):
        """
        Read raw csv to pandas DF
          - type quotes or trades
        """
        root_path = 'crypto-data/bitmex/'
        if style == 'quotes':
            nms = ['timestamp', 'bid', 'ask', 'bidsize', 'asksize']
        elif style == 'trans':
            nms = ['timestamp', 'ordertype', 'volume', 'price', 'ticktype']
        else:
            raise ValueError("Unknown data processing type. Supported types are 'quotes' and ' trans'.")

        path = os.path.join(root_path, style + '/', ticker + '/', date + '.csv')
        self.logger.info(f'Loading {path}')
        obj = bucket.Object(path).get()

        df = pd.read_csv(obj['Body'], header=None, names=nms, parse_dates=True)
        df.index = pd.to_datetime(df.timestamp)
        df = df.drop(labels='timestamp', axis=1)
        if style == 'quotes':
            df.bid = pd.to_numeric(df.bid, errors='coerce')
            df.ask = pd.to_numeric(df.ask, errors='coerce')
            df.bidsize = pd.to_numeric(df.bidsize, errors='coerce')
            df.asksize = pd.to_numeric(df.asksize, errors='coerce')
        else:
            df.volume = pd.to_numeric(df.volume, errors='coerce')
            df.price = pd.to_numeric(df.price, errors='coerce')
        return df

    def get_instruments(self, style='trans'):
        if style == 'quotes':
            dirty_inst_list = [key.key.split('/')[3] for key in
                               bucket.objects.filter(Prefix='crypto-data/bitmex/quotes/')]
        elif style == 'trans':
            dirty_inst_list = [key.key.split('/')[3] for key in
                               bucket.objects.filter(Prefix='crypto-data/bitmex/trans/')]
        else:
            raise ValueError("Supported style are limitted to 'trans' and 'quotes'.")
        unique_list = list(set(dirty_inst_list))
        self.symbols = unique_list
        return unique_list

    def get_availability(self, instrument, style='trans', verbose=True, return_type='series'):
        if style not in ['trans', 'quotes']:
            raise ValueError("Supported style are limitted to 'trans' and 'quotes'.")
        else:
            path = 'crypto-data/bitmex/' + style + '/' + instrument
        try:
            string_dates = [key.key.split('/')[-1].split('.')[0] for key in bucket.objects.filter(Prefix=path)]
            avail = pd.Series(pd.to_datetime(string_dates).sort_values())
            start_date = avail.iloc[0].date()
            end_date = avail.iloc[-1].date()
            missing = pd.Series(pd.date_range(start=start_date, end=end_date).difference(avail))
            self.avail_dict[instrument] = string_dates
        except:
            raise ValueError('Symbol not available.')

        if verbose:
            self.logger.info(f'{instrument}')
            self.logger.info(f'Total days: {len(avail)}')
            self.logger.info(
                f'Missing days: {len(missing)}, i.e., {(len(missing)) / (len(missing) + len(avail)) * 100:.2f} % missing.')
            self.logger.info(f'First date of data: {start_date}')
            self.logger.info(f'Last date of data: {end_date}')
        if return_type == 'series':
            return avail, missing
        elif return_type == 'string':
            return string_dates, missing
        else:
            raise ValueError('Unknown return type.')

    def download_symbol(self, symbol, style='trans', overwrite=False):
        """
        Downloads full history for specified symbol.
        Args:
            symbol: str
            style: str
            overwrite: bool

        Returns:
            None
        """
        if style not in ['quotes', 'trans']:
            raise ValueError("Unrecognized style -- use either 'quotes' or 'trans'.")
        symbol_path = os.path.join(self.download_path, symbol, style)

        if not os.path.exists(symbol_path):
            os.makedirs(symbol_path)
        else:
            if overwrite:
                shutil.rmtree(symbol_path)  # Removes all the subdirectories!
                os.makedirs(symbol_path)
            else:
                self.logger.warning(f"Data folder {symbol_path} has been already initialized. For reinitialization"
                                    f"use argument 'overwrite=True'. Skipping...")

        avail, missing = self.get_availability(symbol, style, verbose=False, return_type='string')
        for d in avail:
            file_path = os.path.join(symbol_path, d + '.parquet')
            if not os.path.exists(file_path):
                df = self.read_boto_obj_to_df(symbol, d, style)
                try:
                    df.to_parquet(file_path)
                except:
                    self.logger.warning(f"Unable to download {file_path}. Skipping...")
                    self.skipped.append(file_path)
                self.logger.info(f'File {file_path} downloaded.')
            else:
                self.logger.warning("File '{file_path}' already exists. Skipping.")
        return

    def download_database(self, symbols):
        # paralelization is not necessary here as download will be throttled by the current task in progress -- I guess

        for s in symbols:
            self.download_symbol(s, style='trans')
            self.download_symbol(s, style='quotes')
            self.logger.info(f'<============ Symbol {s} completely downloaded. ============>')

        self.logger.info(f"Download completed except for {self.skipped}.")
        if self.skipped:
            with open('skipped_files.txt', 'w') as filehandle:
                filehandle.writelines("%s\n" % place for place in self.skipped)





        # symbol_path = os.makedirs(symbol_path)





