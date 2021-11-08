import phoenixdb
import phoenixdb.cursor

database_url = 'http://master.e4rlearning.com:8765'

conn = phoenixdb.connect(database_url,autocommit=True)
cursor = conn.cursor()

cursor.execute('select * from us_population')
print(cursor.fetchall())