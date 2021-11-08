import phoenixdb.cursor
import time
from datetime import  datetime

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


database_url = 'http://master.e4rlearning.com:8765'
# pconn = phoenixdb.connect(database_url, autocommit=True)
pconn = phoenixdb.connect(database_url)
cursor = pconn.cursor()

create_fotable_statement = '''
CREATE TABLE IF NOT EXISTS FOTABLEPH(
    SYMBOL VARCHAR NOT NULL,
    EXPIRY  DATE NOT NULL,
    OPTIONTYPE CHAR(2) NOT NULL,
    STRIKEPRICE DECIMAL NOT NULL,
    TRADEDATE DATE NOT NULL,
    OPENPR DECIMAL,
    HIGHPR DECIMAL,
    LOWPR DECIMAL,
    CLOSEPR DECIMAL,
    SETTLEPR DECIMAL,
    CONTRACTS INTEGER,
    VLAKH DECIMAL,
    OI INTEGER,
    CHGOI INTEGER,
    CONSTRAINT pk_fotable PRIMARY KEY (SYMBOL, EXPIRY, OPTIONTYPE, STRIKEPRICE, TRADEDATE)
)'''

cursor.execute(create_fotable_statement)
pconn.commit()

fo_data_dir = 'D:/findataf/fo/focsv'
fo_test_file = fo_data_dir + '/fo01JAN2018bhav.csv'

beg_time = time.time()
fofile = open(fo_test_file)
fofile.readline()
line = fofile.readline()
batch_size = 2000
batch_no = 0

while line != '':
    counter = 0
    while counter < batch_size  and line != '':
        # print(line)
        line_parts = line.split(',')
        # print(line_parts)
        # create the row key - symbol, expiry, option type, strike price, trade date
        upsert_statement = """
        upsert into FOTABLEPH(
        SYMBOL,EXPIRY,OPTIONTYPE,STRIKEPRICE,TRADEDATE,
        OPENPR ,HIGHPR ,LOWPR ,CLOSEPR ,SETTLEPR ,
        CONTRACTS,VLAKH,OI,CHGOI
        )
        values (?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?)
        """
        line_parts_tuple =  (
            line_parts[1],
            date_to_datetime(line_parts[2]),
            line_parts[4], float(line_parts[3]),
            date_to_datetime(line_parts[14]),
            float(line_parts[6]),float(line_parts[5]),float(line_parts[7]),
            float(line_parts[8]), float(line_parts[9]), int(line_parts[10]),
            float(line_parts[11]), int(line_parts[12]), int(line_parts[13])
        )
        # print(upsert_statement)
        # print(line_parts_tuple)
        cursor.execute(upsert_statement, line_parts_tuple)
        # print('executed cursor statement and counter is {} and batch_size is {}'.format(counter,batch_size))
        if counter % 100 == 0:
            print('processed 100 rows')
        if counter == batch_size - 1:
            # hbase_client.client.mutateRows(table_name,batches, {})
            batch_no  += 1
            print('committing batch no ', batch_no)
            pconn.commit()
        counter += 1
        line = fofile.readline()
    if line == '':
        pconn.commit()

fofile.close()
pconn.close()
end_time = time.time()
print('with batch size of {} took {} to load'.format(batch_size, end_time - beg_time))
