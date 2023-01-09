import happybase
from collections import OrderedDict

conn = happybase.Connection(host='master.e4rlearning.com',
                            port=9090,
                            autoconnect=True
                            )
# open connection
conn.open()

# print tables
print(conn.tables())

# create a table
table_to_create = 'mytable'
if table_to_create not in [x.decode() for x in conn.tables()]:
    conn.create_table(
        table_to_create,
        {'cf1': dict(max_versions=10),
         'cf2': dict(max_versions=1, block_cache_enabled=False),
         'cf3': dict(),  # use defaults
         }
    )

table_to_manipulate = 'mytable'
table = conn.table(table_to_manipulate)

# for x in table.scan():
#     print(str(x[0]))
#     for key, data in  x[1].items():
#         print(str(key))
#         print(str(data))

# for y in range(len(x)):
#     print(type(x[y]))
#     print(x[y])
# for key, data in x[y]:
#     print(str(key))
#     for item in data.items():
#         print(str(item))

# a single put
table.put(b'some-row', {b'cf1:c1': b'some-row-val1'})

# add on a series of puts
for x in range(10):
    for y in range(10):
        table.put("row-" + str(x),
                  {
                      'cf1:c{}{}'.format(x, y).encode(): 'cf1-ideval-{}{}'.format(x, y).encode(),
                      'cf2:c{}{}'.format(x, y).encode(): 'cf2-ideval-{}{}'.format(x, y).encode(),
                      'cf3:c{}{}'.format(x, y).encode(): 'cf3-ideval-{}{}'.format(x, y).encode()
                  })

# get a single row
print(table.row('row-0'))

# get multiple rows
print(table.rows(['row-0', 'row-1', 'row-2']))

# get rows as dictionary
print(dict(table.rows(['row-0', 'row-1', 'row-2'])))

# get rows as ordered dictionary
print(OrderedDict(table.rows(['row-1', 'row-2', 'row-0'])))

# get row and specific colum family
print(table.row('row-1',columns=['cf1']))

# get row and specific colum family and cells
print(table.row('row-1',columns=['cf1:c10', 'cf1:c15']))

# include timestamp
print(table.row('row-1',columns=['cf1'],include_timestamp=True))

# to get versions of cells use table.cells
print(table.cells('row-1',column='cf1',versions=5,include_timestamp=True))

# scan over a table
for key, data in table.scan():
    print(key, data)

# provide a start row to the scan
for key, data in table.scan(row_start='row-1'):
    print(key, data)

# provide start and stop rows to the scan
for key, data in table.scan(row_start='row-1',row_stop='row-1'):
    print(key, data)

# provide a key prefix to the scan
for key, data in table.scan(row_prefix=b'row-1'):
    print(key, data)

# delete a table row
table.delete('row-0')

# performing batch mutations
b=table.batch()
b.put('row-b1', {'cf1:cb1': 'batch-val1' })
b.put('row-b1', {'cf1:cb2': 'batch-val2' })
b.put('row-b1', {'cf1:cb3': 'batch-val3' })
b.put('row-b2', {'cf1:cb1': 'rb2-batch-val1', 'cf1:cb2': 'rb2-batch-val2', 'cf1:cb3': 'rb2-batch-val3' })
b.send()

# performing batch mutations using with context manager
# eliminates the need for batch send
#  The batch is automatically applied when the with code block terminates,
#  even in case of errors somewhere in the with block,
#  so it behaves basically the same as a try/finally
with table.batch() as b:
    b.delete('row-b1')
    b.delete('row-b2')

table.rows(rows=['row-b1', 'row-b2'])

with table.batch(batch_size=1000) as b:
    for i in range(1200):
        # this put() will result in two mutations (two cells)
        b.put('row-%04d' % i, {
            'cf1:col1': 'v1',
            'cf1:col2': 'v2',
        })

# icnrementing, decrementing counters
print(table.counter_inc('row-key', 'cf1:counter'))  # prints 3
print(table.counter_inc('row-key', 'cf1:counter'))  # prints 2
print(table.counter_inc('row-key', 'cf1:counter'))  # prints 1

print(table.counter_dec(b'row-key', b'cf1:counter'))  # prints 2


# Filters
# KeyOnlyFilter
# This filter doesn’t take any arguments. It returns only the key component of each key-value.
print([x for x in table.scan(filter="KeyOnlyFilter()")])
# scan 'mytable', {FILTER => 'KeyOnlyFilter()'}

# FirstKeyOnlyFilter
# This filter doesn’t take any arguments. It returns only the first key-value from each row.
print([x for x in table.scan(filter="FirstKeyOnlyFilter()")])
print(table.row('row-7'))

# scan 'mytable', {FILTER => 'FirstKeyOnlyFilter()'}
# ColumnPrefixFilter
# This filter takes one argument – a column prefix.
# It returns only those key-values present in a column that starts with the specified column prefix
# The column prefix must be of the form: “qualifier”
table.put('mytable', {'cf1:colprefix1':'cprefixval1', 'cf2:colprefix2':'cprefixval2'})
print([x for x in table.scan(filter="ColumnPrefixFilter('colprefix')")])
# scan 'mytable', {FILTER => "ColumnPrefixFilter('colprefix')"}

# MultipleColumnPrefixFilter
# This filter takes a list of column prefixes.
# It returns key-values that are present in a column that starts with any of the specified column prefixes.
# Each of the column prefixes must be of the form: “qualifier”
table.put('mytable', {'cf1:altprefix1':'altprefixval1', 'cf2:altprefix2':'altprefixval2'})
print([x for x in table.scan(filter="MultipleColumnPrefixFilter('colprefix', 'altprefix')")])

# ColumnCountGetFilter
# This filter takes one argument – a limit. It returns the first limit number of columns in the table.
print([x for x in table.scan(filter="ColumnCountGetFilter(5) AND PrefixFilter('row-7')")])
# scan 'mytable', {FILTER =>"ColumnCountGetFilter(5) AND PrefixFilter('row-7')"}

# PageFilter
# This filter takes one argument – a page size. It returns page size number of rows from the table
print([x for x in table.scan(filter="PageFilter(10)")])
# scan 'mytable', {FILTER =>"PageFilter(10)"}

# ColumnPaginationFilter
# This filter takes two arguments – a limit and offset.
# It returns limit number of columns after offset number of columns. It does this for all the rows.
print([x for x in table.scan(filter="ColumnPaginationFilter(3,2) AND PrefixFilter('row-7')")])
# scan 'mytable', {FILTER =>"ColumnPaginationFilter(3,2) AND PrefixFilter('row-7')"}

# InclusiveStopFilter
# This filter takes one argument – a row key on which to stop scanning.
# It returns all key-values present in rows up to and including the specified row.
print([x for x in table.scan(filter="InclusiveStopFilter('row-2')")])
# scan 'mytable', {FILTER =>"InclusiveStopFilter('row-2')"}

# RowFilter
# This filter takes a compare operator and a comparator.
# It compares each row key with the comparator using the compare operator and if the comparison returns true,
# it returns all the key-values in that row.
print([x for x in table.scan(filter="RowFilter(=,'binary:row-7')")])
# scan 'mytable', {FILTER =>"RowFilter(=,'binary:row-7')"}
print([x for x in table.scan(filter="RowFilter(=,'substring:row-7')")])
# scan 'mytable', {FILTER =>"RowFilter(=,'substring:row-7')"}

# TimeStampsFilter
# This filter takes a list of timestamps.
# It returns those key-values whose timestamps matches any of the specified timestamps.
print([x for x in table.scan(filter="RowFilter(=,'substring:row-7')",include_timestamp=True)])
# scan 'mytable', {FILTER =>"RowFilter(=,'substring:row-7')"}
print( [x for x in table.scan(filter="TimestampsFilter(1635645408997)", include_timestamp=True)])

# Family Filter
# This filter takes a compare operator and a comparator.
# It compares each column family name with the comparator using the compare operator and
# if the comparison returns true, it returns all the Cells in that column family.
print([x for x in table.scan(filter="FamilyFilter(=,'substring:cf1')")])
# scan 'mytable', {FILTER =>"FamilyFilter(=,'substring:cf1')"}
print([x for x in table.scan(filter="FamilyFilter(=,'substring:cf1') AND RowFilter(=,'substring:row-7')")])
# scan 'mytable', {FILTER =>"FamilyFilter(=,'substring:cf1') AND RowFilter(=,'substring:row-7')"}

# QualifierFilter
# This filter takes a compare operator and a comparator.
# It compares each qualifier name with the comparator using the compare operator and
# if the comparison returns true, it returns all the key-values in that column.
print([x for x in table.scan(filter="QualifierFilter(=,'substring:c70')")])
# scan 'mytable', {FILTER =>"QualifierFilter(=,'substring:c70')"}

# ValueFilter
# This filter takes a compare operator and a comparator.
# It compares each value with the comparator using the compare operator and
# if the comparison returns true, it returns that key-value.
print([x for x in table.scan(filter="ValueFilter(=,'regexstring:^ideval')")])
# scan 'mytable', {FILTER =>"ValueFilter(=,'regexstring:^ideval')"}

# DependentColumnFilter
# This filter takes two arguments – a family and a qualifier.
# It tries to locate this column in each row and
# returns all key-values in that row that have the same timestamp.
# If the row doesn’t contain the specified column –
# none of the key-values in that row will be returned.
print([x for x in table.scan(filter="DependentColumnFilter('cf1','c11')")])
# scan 'mytable', {FILTER =>"DependentColumnFilter('cf1','c11')"}

# SingleColumnValueFilter
# This filter takes a column family, a qualifier, a compare operator and a comparator.
# If the specified column is not found – all the columns of that row will be emitted.
# If the column is found and the comparison with the comparator returns true,
# all the columns of the row will be emitted.
# If the condition fails, the row will not be emitted.
print([x
       for x in table.scan(
        filter="SingleColumnValueFilter('cf1','c11',=,'regexstring:cf1-ideval-11')"
                              " AND RowFilter(=,'regexstring:row-[12]$')"
    )])
# scan 'mytable', {FILTER =>"SingleColumnValueFilter('cf1','c11',=,'regexstring:cf1-ideval-11') AND RowFilter(=,'regexstring:row-[12]$')"}
print([x
       for x in table.scan(
        filter="SingleColumnValueFilter('cf1','c11',!=,'regexstring:cf1-ideval-11')"
               " AND RowFilter(=,'regexstring:row-[12]$')"
    )])
# scan 'mytable', {FILTER =>"SingleColumnValueFilter('cf1','c11',!=,'regexstring:cf1-ideval-11') AND RowFilter(=,'regexstring:row-[12]$')"}

# SingleColumnValueExcludeFilter
# This filter takes the same arguments and behaves same as SingleColumnValueFilter – however,
# if the column is found and the condition passes,
# all the columns of the row will be emitted except for the tested column value.
print([x
       for x in table.scan(
        filter="SingleColumnValueExcludeFilter('cf1','c11',=,'regexstring:cf1-ideval-11')"
               " AND RowFilter(=,'regexstring:row-[12]$')"
    )])
# scan 'mytable', {FILTER =>"SingleColumnValueExcludeFilter('cf1','c11',=,'regexstring:cf1-ideval-11') AND RowFilter(=,'regexstring:row-[12]$')"}

# ColumnRangeFilter
# This filter is used for selecting only those keys with columns that are between minColumn and maxColumn. I
# t also takes two boolean variables to indicate whether to include the minColumn and maxColumn or not.
print([
    x for x in table.scan(filter="ColumnRangeFilter('c11',True,'c15',True)")
])
# scan 'mytable', {FILTER =>"ColumnRangeFilter('c11',True,'c15',True)"}

# Logical Filters SKIP and WHILE
#SKIP - For a particular row, if any of the key-values fail the filter condition, the entire row is skipped.
print([x for x in table.scan(filter="SKIP ValueFilter(=,'regexstring:.*ideval-9')")])
# scan 'mytable', {FILTER =>"SKIP ValueFilter(=,'regexstring:.*ideval-9')"}
# WHILE - For a particular row, key-values will be emitted
# until a key-value is reached that fails the filter condition.
print([
    x for x in table.scan(filter="WHILE RowFilter(!=,'substring:row-0004')")
])
# scan 'mytable', {FILTER =>"WHILE RowFilter(!=,'substring:row-0004')"}
