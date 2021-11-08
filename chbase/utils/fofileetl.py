
def fdp(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10",
                "NOV": "11", "DEC": "12"}
    tdt = dt.upper().replace(mname, calendar[mname])
    return tdt[tdt.rindex('-') + 1:len(dt)]+ '-' + tdt[3:5] + '-' +tdt[:2]

def transform(inputfile,outputfile):
    outfile = open(outputfile, 'w')
    infile = open(inputfile)
    line = infile.readline()
    outfile.write('SYMBOL,EXPIRY,OPTIONTYPE,STRIKEPRICE,TRADEDATE,OPENPR,HIGHPR,'+
                  'LOWPR,CLOSEPR,SETTLEPR,CONTRACTS,VLAKH,OI,CHGOI\n')
    while line != '':
        line = infile.readline()
        # print(line)
        if line != '':
            line_parts = line.split(',')
            line_corrected = ','. join( [line_parts[1],
                                        fdp(line_parts[2]),
                                        line_parts[4], line_parts[3],
                                        fdp(line_parts[14]),
                                        line_parts[6],line_parts[5],line_parts[7],
                                        line_parts[8], line_parts[9], line_parts[10],
                                        line_parts[11], line_parts[12], line_parts[13]]
                                )
            # print(line_corrected)
            outfile.write(line_corrected + '\n')
    print('transformed {} file and wrote it to destination {}'.format(infile,outfile))
    outfile.close()
    infile.close()

# transform('D:/findataf/fo/focsv/fo01JAN2018bhav.csv', 'D:/tmp/fofp/fo01JAN2018bhav.csv')

def transform_directory_files_etl(indir, outdir):
    import os
    for file in os.listdir(indir):
        infile = os.path.join(indir, file)
        outfile = os.path.join(outdir, file)
        transform(infile, outfile)

transform_directory_files_etl('D:/findataf/201819/fo', 'D:/tmp/fofp')

# load using psql.py
# phoenix-hbase-2.4.0-5.1.2-bin/bin/psql.py -t FOTABLEPSQL -h in-line master:2182 /vagrant/fo01JAN2018bhav.csv

# load using mapreduce
# mapreduce will not work with headers
# sed 1d fo01JAN2018bhav.csv > fo01JANNoHeader.csv
# hadoop jar \
# phoenix-hbase-2.4.0-5.1.2-bin/phoenix-client-hbase-2.4.0-5.1.2.jar \
# org.apache.phoenix.mapreduce.CsvBulkLoadTool --table FOTABLEPSQL --input fo01JANNoHeader.csv -z master:2182