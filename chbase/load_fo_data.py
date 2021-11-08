import sys
import time
import os
from datetime import datetime
import happybase

sys.path.append(os.path.join(sys.path[0], '../thrift/gen-py'))

conn = happybase.Connection(host='master.e4rlearning.com',
                                    port=9090,
                                    autoconnect=True
                            )
conn.open()

fo_data_dir = 'D:/findataf/fo/focsv'
fo_test_file = fo_data_dir + '/fo03JAN2018bhav.csv'

table_to_create = 'FOTABLEH'
if table_to_create not in [x.decode() for x in conn.tables()]:
    conn.create_table(
        table_to_create,
        {'D': dict([('max_versions', 5)])}
    )

def mnameToNo(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10",
                "NOV": "11", "DEC": "12"}

    return dt.upper().replace(mname, calendar[mname])


def date_to_datetime(dt):
    mtndate = mnameToNo(dt)
    return datetime(int(mtndate[mtndate.rindex('-') + 1:len(mtndate)]),
                    int(mtndate[3:5]),
                    int(mtndate[:2]))


# create FOTABLE in hbase
beg_time = time.time()
fofile = open(fo_test_file)
fofile.readline()
line = fofile.readline()
batch_size = 200
table = conn.table(table_to_create)
b = table.batch()

while line != '':
    counter = 0
    while counter < batch_size - 1 and line != '':
        # print(line)
        line_parts = line.split(',')
        # print(line_parts)
        # create the row key - symbol, expiry, option type, strike price, trade date
        rkey = 's'+line_parts[1] +\
               'e' + str(int(date_to_datetime(line_parts[2]).timestamp())) +\
               'o' + line_parts[4] +\
               'p' + line_parts[3] +\
               't' + str(int(date_to_datetime(line_parts[14]).timestamp()))
        row_dict = {}
        mutations = []
        row_dict['D:OPR'] = line_parts[5]
        row_dict['D:HPR'] = line_parts[6]
        row_dict['D:LPR'] = line_parts[7]
        row_dict['D:CPR'] = line_parts[8]
        row_dict['D:SPR'] = line_parts[9]
        row_dict['D:CNTRS'] = line_parts[10]
        row_dict['D:VLKH'] = line_parts[11]
        row_dict['D:OI'] = line_parts[12]
        row_dict['D:CHGOI'] = line_parts[13]
        print(rkey)
        # batches.append(BatchMutation(rkey.encode(),mutations))
        b.put(rkey, row_dict)
        if counter == batch_size - 1:
            # hbase_client.client.mutateRows(table_name,batches, {})
            print('sending batch')
            b.send()
            b = table.batch()
        counter += 1
        line = fofile.readline()
    if line == '':
        # hbase_client.client.mutateRows(table_name,batches, {})
        b.send()
        b = table.batch()

fofile.close()
end_time = time.time()
print('with batch size of {} took {} to load'.format(batch_size, end_time - beg_time))

# with batch size of 10 took 22.14592456817627 to load
# with batch size of 50 took 12.827240943908691 to load
# with batch size of 100 took 11.955453872680664 to load
# with batch size of 200 took 10.412480354309082 to load
# with batch size of 500 took 10.544628620147705 to load
# with batch size of 1000 took 9.70706033706665 to load
# with batch size of 2000 took 11.103074073791504 to load
conn.close()