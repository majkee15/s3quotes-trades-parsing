from parser_hft_sc import *

print(f"Available trade data for :{get_instruments('trans')}")
print(f"Available quote data for :{get_instruments('quotes')}")
get_availability('BCH', style='trans', verbose=True)

btc_trade = read_boto_obj_to_df('XBTUSD', '2020-11-20', 'trans')

print(btc_trade.head())