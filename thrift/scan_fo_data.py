from hbase.Hbase import ColumnDescriptor, Mutation, BatchMutation
from thrift_client import ThriftClient
from datetime import  datetime

host = "master.e4rlearning.com"
port = 9090


def getDataForASymbolForOneDate(symbol,expiry,table):
    hbase_client = ThriftClient(host, port, False)
    hbase_client.transport.open()
    scanned_rows = []
    routs = []
    rspec = ('s' + symbol + 'e' + str(int(expiry.timestamp()))).encode()
    scanner = hbase_client.client.scannerOpenWithPrefix(
        table, rspec, [b'D'],{})
    r = hbase_client.client.scannerGet(scanner)
    scanned_rows.append(r)
    while r:
        r = hbase_client.client.scannerGet(scanner)
        scanned_rows.append(r)
    for rno in range(len(scanned_rows)-1):
        routs.append([(col[0].decode(),col[1].value.decode())
                      for col in scanned_rows[rno][0].columns.items()])
    hbase_client.transport.close()
    return routs


getDataForASymbolForOneDate('INFY', datetime(2018, 1, 25), b'FOTABLE')
