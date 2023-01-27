import csv
import psycopg2
import create_engine from sqlalchemy
import pandas

#import files
conn = psycopg2.connect ("host=localhost dbname=Anshory user=postgres password=kepolu69")

cur = conn.cursor ()

#create table
cur.execute 
("""""
    CREATE TABLE IF NOT EXISTS users (
    id integer PRIMARY KEY,    
    email text,
    name text,
    address text)
""")
conn.commit ()

#insert data
with open ('C:/Users/fnofliwar/Desktop/Book_1.csv','r') as f :
    reader = csv.reader (f)
    next (reader)
    for row in reader : 
        cur.execute(
        "INSERT INTO Book_1 VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        row 
        )
conn.commit ()

#insert data (copy)
with open ('C:/Users/fnofliwar/Desktop/Book1_w_phone.csv','r') as f :
    reader = csv.reader (f)
    next (f)
    for row in reader : 
        cur.copy_from (f, 'Book_1', sep=',', columns=('email','name','phone') )

conn.commit ()


#create table from file
df =pd.read_csv('C:/Users/fnofliwar/Desktop/Book1_w_phone.csv')

engine = create_engine('postgressql://postgres:kepolu69@localhost:5432/postgres')
df.to_sql ("from_file_table",engine)

print (df)