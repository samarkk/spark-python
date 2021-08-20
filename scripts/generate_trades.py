import glob
import os
import numpy as np
import pandas as pd
import random
from datetime import datetime
import calendar

# read the cash market files
c_files_path = 'D:/findataf/201819/cm'
# use glob to get all the files
c_files = glob.glob(os.path.join(c_files_path, '*.csv'))
# use pandas read csv to get a dataframe for each file - header by default is true and is the first line
cm_daily_df = (pd.read_csv(f) for f in c_files)
# concatenate the cash  market dataframes into a single dataframe
cm_df = pd.concat(cm_daily_df, ignore_index=True)
# checing an aggregation
print(cm_df.groupby('SYMBOL').agg({'CLOSE': ['min', 'max', 'mean']}))

# follow similar process to create a datafame for the fo files for a particular month
fo_dir_path = 'D:/findataf/201819/fo'
fo_dir_files = glob.glob(os.path.join(fo_dir_path, '*JAN*.csv'))
fo_daily_df = (pd.read_csv(f) for f in fo_dir_files)
fo_df = pd.concat(fo_daily_df, ignore_index=True)
fo_df.describe()

# the future and option symbol list
fo_df_symbols = fo_df.SYMBOL.unique()

# group by symbol, instrument, option type and strike price
# a random row of these with the tarding date as current date
# expiry date as last day of the month if current date is less than 21
# otherwise last thursday of the following month

fodf_grouped = fo_df.groupby(['SYMBOL', 'INSTRUMENT', 'OPTION_TYP', 'STRIKE_PR']).agg({'CLOSE': ['min', 'max', 'mean']}).reset_index()
fodf_grouped.columns = ['symbol', 'instrument', 'option_typ', 'strike_pr', 'mincls', 'maxcls', 'mncls']
fodf_grouped
print(fodf_grouped.iloc[:10])

# need the lot sizes for generating random trades
# first filter only to futures rows
fodf_grouped_only_futs = fodf_grouped[fodf_grouped.instrument.str.startswith('FUT')]

# then generate lotsize as roughly 5 lakh / average closing price rounded to the nearest 10
fodf_lotsizes = fodf_grouped_only_futs.assign(lotsize = lambda x: np.int32(np.ceil(50000 / x.mncls) * 10))

# merge fodf_grouped and fodf_lotsize so that we have the lotsize information available
# to generate the roder size
fodf_with_lotsize = fodf_grouped.merge(fodf_lotsizes.loc[:,['symbol','lotsize']],on='symbol',suffixes=['',''])

# some stuff to find all thursdays for the month
def find_all_thursdays_in_current_month():
    from datetime import  datetime
    from functools import reduce
    today = datetime.today()
    current_year = today.year
    current_month = today.month
    # print(today, current_year, current_month)
    thursdays_3m = [[],[],[]]
    for x in range(1, 32):
        try:
            date_object = datetime(current_year, current_month, x)
            # print(date_object, date_object.weekday())
            if date_object.weekday() == 3:
                thursdays.append(date_object)
        except e as Exception:
            print(e.message)
    last_thursday = reduce(lambda x, y: max(x,y), map(lambda aday: aday.day, thursdays))
    return thursdays, last_thursday
'''
if the weekday for the first date of the month is x
then when is the first thursday - calculations below
if it is 0 then it will be 4 because 0 is monday and add 3 to 1 to get 4 
likewise 1 is tuesday and add 2 to get 3 for first thursday and so on
0	1	4	1+3-0
1	1	3	1+3-1
2	1	2	1+3-2
3	1	1	1+3-3
4	1	7	7-(4-4)
5	1	6	7-(5-4)
6	1	5	7-(6-4)

'''


def expiry_for_current_date():
    '''
    get the current date
    if it is less than 21 take the current month otherwse the next month
    use calendar to get the last date for the month
    for feb 2021 we should get 28 and for aug 2021 31
    check back from the last date and return the first date for which weekday is 4
    '''
    today = datetime.today()
    current_year = today.year
    current_month = 0
    if today.day < 21:
        current_month = today.month
    else:
        current_month = today.month + 1
    print(current_month)
    _, last_day_of_month = calendar.monthrange(current_year, current_month)
    x  = last_day_of_month
    while x > last_day_of_month - 7:
        date_object = datetime(current_year, current_month, x)
        # print(date_object, date_object.weekday())
        if date_object.weekday() == 4:
            return date_object
        else:
            x = x - 1

# format the date so that it matches the format in which we get the nse data
def format_date_nse(date):
    return str(date.day) + '-' + date.strftime('%b').upper() + '-' + str(date.year)

# if were using the dataframe - fodf_with_lotsize record as was, this below would be useful
def format_df_record(dfrec):
    return  str(dfrec.to_list()).replace("[",'').replace("]",'')

# assign expiry date and trade date to variables
# expiry date - last thursday of current or following month and trade date  - today
# if it is a holiday, sunday - ignore - this is a playground

expd = format_date_nse(expiry_for_current_date())
trdate = format_date_nse(datetime.today())

# store the length of the dataframe from which we will pick random records into a variable
recsrange = fodf_with_lotsize.shape[0] - 1

# randomly choose positive, negative and adjust the price be a random percentage between 0 and 10
def generate_random_price(price):
    switch = random.randint(0,2)
    randomp = random.randint(0, 10)
    if switch == 0:
        return price * (1 - randomp / 100)
    else:
        return price * (1 + randomp / 100)

# combine everything to generate a csv random record
# any random record from the fodf_grouped
# with trade date, expiry date, price as describe earlier
# and order size between 1 to 5 times the lot size
def generate_random_rec():
    recno = random.randint(0, recsrange)
    dfrec = fodf_with_lotsize.loc[recno]
    genrec = ','.join([
        trdate,
        dfrec['symbol'],
        dfrec['instrument'],
        expd,
        dfrec['option_typ'],
        str(dfrec['strike_pr']),
        str(random.randint(1,5) * dfrec['lotsize']),
        str(round(generate_random_price(dfrec['mncls']),2))
    ])
    # print(genrec)
    return genrec

# so that one did not have to type this guy in the console
generate_random_rec()

# assigning number of records to generate to a variable
no_of_recs_to_generate = 1000000

# and generate that many number of records
counter = 0
for x in range(no_of_recs_to_generate):
    beg_time = datetime.now()
    generate_random_rec()
    counter += 1
    if counter % 10000 == 0:
        print('generated {} records'.format(counter))
    end_time = datetime.now()
    if counter == no_of_recs_to_generate - 1:
        print('Took {} seconds to generate {} records'\
              .format((end_time - beg_time).seconds, no_of_recs_to_generate))