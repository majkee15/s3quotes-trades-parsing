import os

import boto3
import pandas as pd

# selec AWS profile as environment variabl

os.getenv('AWS_PROFILE', 'dynamodbSobCap')

s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket('sobesice-capital-data')


def get_instruments(style='trans'):
    """

    :param style: string
        supports either trans -- transactions or quotes
    :return:  unique list of symbols available in the S4 bucket
    """
    if style == 'quotes':
        dirty_inst_list = [key.key.split('/')[3] for key in bucket.objects.filter(Prefix='crypto-data/bitmex/quotes/')]
    elif style == 'trans':
        dirty_inst_list = [key.key.split('/')[3] for key in bucket.objects.filter(Prefix='crypto-data/bitmex/trans/')]
    else:
        raise ValueError("Supported style are limitted to 'trans' and 'quotes'.")
    unique_list = list(set(dirty_inst_list))
    return unique_list


def read_boto_obj_to_df(ticker, date, style):
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
    print(path)
    obj = bucket.Object(path).get()

    df = pd.read_csv(obj['Body'], header=None, names=nms, parse_dates=True)
    df.index = pd.to_datetime(df.timestamp)
    return df


def get_availability(instrument, style='trans', verbose=True):
    """

    Args:
        instrument: string describing the symbol id
        style: string specifying return type -- trans or quotes
        verbose: bool

    Returns:
        list of available dates
        list of missing dates
    """
    if style not in ['trans', 'quotes']:
        raise ValueError("Supported style are limitted to 'trans' and 'quotes'.")
    else:
        path = 'crypto-data/bitmex/' + style + '/' + instrument
    avail = pd.Series(pd.to_datetime(
        [key.key.split('/')[-1].split('.')[0] for key in bucket.objects.filter(Prefix=path)]).sort_values())
    start_date = avail.iloc[0].date()
    end_date = avail.iloc[-1].date()
    missing = pd.Series(pd.date_range(start=start_date, end=end_date).difference(avail))

    if verbose:
        print(f'Total days: {len(avail)}')
        print(
            f'Missing days: {len(missing)}, i.e., {(len(missing)) / (len(missing) + len(avail)) * 100:.2f} % missing.')
        print(f'First date of data: {start_date}')
        print(f'Last date of data: {end_date}')

    return avail, missing

