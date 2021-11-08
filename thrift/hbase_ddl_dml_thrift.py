import sys
import time
import os
import random
import string

sys.path.append(os.path.join(sys.path[0], 'gen-py'))

from hbase.Hbase import Client, ColumnDescriptor, Mutation, BatchMutation
from thrift_client import ThriftClient

def printVersions(row, versions):
    print ("row: " + str(row) + ", values: ",)
    for cell in versions:
        print (str(cell.value) + "; ",)
    print()

def printRow(entry):
    print ("row: " + str(entry.row) + ", cols:",)
    for k in sorted(entry.columns):
        print (str(k) + " => " + str(entry.columns[k].value),)
    print()

hbase_client = ThriftClient('master.e4rlearning.com', 9090, False)
hbase_client.connect()

columns = []
col = ColumnDescriptor()
col.name = b'f1'
col.maxVersions = 10
columns.append(col)

col = ColumnDescriptor()
col.name = b'f2'
columns.append(col)

# create a table
hbase_client.create_table(b'tmp1', columns)


def generate_random_string(nletters):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=nletters))


def single_put(table, rowkey, colfam, qualifier, hbclient):
    mutations = [Mutation(column=(colfam + ':' + qualifier).encode(),
                          value=generate_random_string(10).encode())]
    hbclient.client.mutateRow(table, rowkey, mutations, {})


# add a single put
single_put(b'tmp1', b'r1', 'f1', 'c1', hbase_client)


# ###############################################
# apply multiple mutations in a single put
# ###############################################

def create_list_of_mutations(colfam, qualifier, nmutatons):
    list_mutations = []
    for x in range(nmutatons):
        list_mutations.append(Mutation(column=(colfam + ':' + qualifier).encode(),
                                       value=generate_random_string(10).encode()))
    return list_mutations


for x in range(10):
    for fam in ['f1', 'f2']:
        for q in range(10):
            hbase_client.client.mutateRow(b'tmp1', ('r' + str(x)).encode(),
                                          create_list_of_mutations(fam, 'c' + str(q), 10), {})

# ###############################################
#  create row batches for mutation
# ###############################################

mutations_batch = []

for row in range(10):
    for qualifier in range(10):
        for fam in ['f1', 'f2']:
            mutations = create_list_of_mutations(fam, str(qualifier), 10)
            mutations_batch.append(BatchMutation(('rb-' + str(row)).encode(), mutations))

hbase_client.client.mutateRows(b'tmp1', mutations_batch, {})

# ###############################################
# put multiple versions for a cell
# ###############################################

for x in range(10):
    mutation = Mutation(column=b'f1:v1', value=('v' + str(x)).encode())
    hbase_client.client.mutateRow(b'tmp1', b'rv1', [mutation], {})

# in the shell it can be gotten through
# get 'tmp1', 'rv1',  {COLUMN => 'f1:v1', VERSIONS => 10
# or
# scan 'tmp1', {VERSIONS => 10
# will have to search for  specific rowkey here

# ###############################################
#  with Ts  timestamp example
# ###############################################

hbase_client.client.mutateRowTs(tableName=b'tmp1',
                                attributes={}, row=b'rts1',
                                mutations=[Mutation(column=b'f1:ts1',value=b'mutation_with_timestamp')],
                                timestamp=int(round(time.time())) * 1000)


##############################################
# get a row and print
##############################################
printRow(hbase_client.client.getRow(b'tmp1',b'r1',{})[0])

###############################################
# get a row and column
###############################################
hbase_client.client.get(b'tmp1',b'r1',b'f1',{})


##############################################
# get a row and column family
##############################################
for column in hbase_client.client.getRowWithColumns(b'tmp1',b'r1', [b'f1'],{}):
    print(column)

##############################################
# get versions of cells for a row
##############################################
hbase_client.client.getVer(b'tmp1',b'rv1',b'f1:v1',10,{})
printVersions(b'rv1',hbase_client.client.getVer(b'tmp1',b'rv1',b'f1:v1',10,{}))

##############################################
# scan a table using start and stop rows
##############################################
columnNames=[]
for (col, desc) in hbase_client.client.getColumnDescriptors(b'tmp1').items():
    print ("column with name: "+str(desc.name))
    print (desc)
    columnNames.append(desc.name[0:len(desc.name)-1])

scanner=hbase_client.client.scannerOpenWithStop(b'tmp1',b'r1',b'r2',columnNames,{})

r = hbase_client.client.scannerGet(scanner)
print('staring scanner with start and stop rows')
while r:
    printRow(r[0])
    r = hbase_client.client.scannerGet(scanner)

##############################################
# scan a table using prefix
##############################################
print('staring scanner with prefix')
scanner_with_prefix = hbase_client.client.scannerOpenWithPrefix(b'tmp1',b'rb',columnNames,{})
r_prefix = hbase_client.client.scannerGet(scanner_with_prefix)
while r_prefix:
    printRow(r_prefix[0])
    r_prefix = hbase_client.client.scannerGet(scanner_with_prefix)


