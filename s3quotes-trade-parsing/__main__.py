# __main__.py
# downloads and constructs the file structured .parquet database

from downloader import Downloader


perpetuities = ['ETHUSD', 'LINKUSDT', 'LTCUSD', 'XBTUSD', 'XRPUSD', 'BCHUSD']
dldr = Downloader()
print(f'Available instruments: {dldr.get_instruments()}')
print('Downloading all the perpetuities.')

#ldr.get_availability('XBTUSD', 'trans', verbose=True)

dldr.download_database(perpetuities)



#print(f'avail dict:{ldr.avail_dict}')

#ldr.download_symbol('XBTUSD', overwrite=True)


# df = ldr.read_boto_obj_to_df('XBTUSD', '2020-11-20', 'trans')
# print(df.head())
