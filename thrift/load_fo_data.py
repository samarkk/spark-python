import sys
import time
import os
from datetime import datetime

sys.path.append(os.path.join(sys.path[0], 'gen-py'))

from hbase.Hbase import ColumnDescriptor, Mutation, BatchMutation
from thrift_client import ThriftClient

hbase_client = ThriftClient('master.e4rlearning.com', 9090, False)
hbase_client.transport.open()

fo_data_dir = 'D:/findataf/fo/focsv'
fo_test_file = fo_data_dir + '/fo01JAN2018bhav.csv'

columns = []
col = ColumnDescriptor()
col.name = b'D'
columns.append(col)
table_name = b'FOTABLE'
hbase_client.create_table(table_name, columns)

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
while line != '':
    counter = 0
    batches = []
    while counter < batch_size  and line != '':
        print(line)
        line_parts = line.split(',')
        # print(line_parts)
        # create the row key - symbol, expiry, option type, strike price, trade date
        rkey = 's'+line_parts[1] +\
               'e' + str(int(date_to_datetime(line_parts[2]).timestamp())) +\
               'o' + line_parts[4] +\
               'p' + line_parts[3] +\
               't' + str(int(date_to_datetime(line_parts[14]).timestamp()))
        mutations = []
        openpr_mutation = Mutation(column=b'D:OPR', value=line_parts[5].encode())
        mutations.append(openpr_mutation)
        highpr_mutation = Mutation(column=b'D:HPR', value=line_parts[6].encode())
        mutations.append(highpr_mutation)
        lowpr_mutation = Mutation(column=b'D:LPR', value=line_parts[7].encode())
        mutations.append(lowpr_mutation)
        closepr_mutation = Mutation(column=b'D:CPR', value=line_parts[8].encode())
        mutations.append(closepr_mutation)
        settlepr_mutation = Mutation(column=b'D:SPR', value=line_parts[9].encode())
        mutations.append(settlepr_mutation)
        contracts_mutation = Mutation(column=b'D:CNTRS', value=line_parts[10].encode())
        mutations.append(contracts_mutation)
        vlakh_mutation = Mutation(column=b'D:VLKH', value=line_parts[11].encode())
        mutations.append(vlakh_mutation)
        oi_mutation = Mutation(column=b'D:OI', value=line_parts[12].encode())
        mutations.append(oi_mutation)
        chgoi_mutation = Mutation(column=b'D:CHGOI', value=line_parts[13].encode())
        mutations.append(chgoi_mutation)
        # print(rkey)
        batches.append(BatchMutation(rkey.encode(),mutations))
        if counter == batch_size - 1:
            # print('Batches, ', batches)
            print('Sending batch')
            hbase_client.client.mutateRows(table_name,batches, {})
            batches = []
        line = fofile.readline()
        counter += 1
    if line == '':
        hbase_client.client.mutateRows(table_name,batches, {})
        batches = []
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
hbase_client.transport.close()