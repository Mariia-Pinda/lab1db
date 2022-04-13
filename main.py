import csv
import psycopg2
import os
import subprocess

POSTGRES_DB = 'labdb1_DB'
exp = 'setx POSTGRES_DB "labdb1_DB"'
subprocess.Popen(exp, shell=True).wait()

username = 'pinda_maria'
password = '111'
database = 'labdb1_DB'

input_csv_file1 = 'Odata2019File.csv'
input_csv_file2 = 'Odata2020File.csv'

query_create = '''
CREATE TABLE IF NOT EXISTS english19_20
(
  id      char(50)  NOT NULL ,
  eng_region char(50) ,
  engtest char(20) ,
  engtest_status char(20) ,
  engball numeric ,
  year numeric ,
  num numeric ,
  numcsv numeric ,
  CONSTRAINT pk_id PRIMARY KEY (id)
);
'''

query_ins = '''
insert into english19_20(id, eng_region, engtest, engtest_status, engball, year, num, numcsv) values (%s, %s, %s, %s, %s, %s, %s, %s) 
'''

query_count_rows = '''
select count(*) from english19_20
'''

query_find_last = '''
select numcsv from english19_20 where num = (select count(*) from english19_20)
'''

query_start_sec = '''
select count(*) from english19_20 where year=2020
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=os.getenv("SQL_HOST"), port=os.getenv("SQL_PORT"))

with conn:
    cur = conn.cursor()
    cur.execute(query_create)
    cur.execute(query_count_rows)
    count_records = cur.fetchone()[0]

    with open(input_csv_file1, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=';')
        cur.execute(query_start_sec)
        count_st_last = int(float(cur.fetchone()[0]))
        if count_st_last == 0:
            if count_records != 0:
                cur.execute(query_find_last)
                count_csv = int(cur.fetchone()[0])
                for i in range(count_csv):
                    next(reader)
            else:
                count_csv = 0

            for row in reader:
                if row['engTestStatus'] == 'Зараховано':
                    values1 = (row['OUTID'], row['engPTRegName'], row['engTest'], row['engTestStatus'], float(row['engBall100'].replace(",", ".")), 2019, count_records+1, count_csv+1)
                    cur.execute(query_ins, values1)
                    count_records += 1
                    conn.commit()
                count_csv += 1

    with open(input_csv_file2, 'r') as inf:
        reader = csv.DictReader(inf, delimiter=';')
        cur.execute(query_find_last)
        count_csv = int(float(cur.fetchone()[0]))
        cur.execute(query_start_sec)
        count_st_last = int(float(cur.fetchone()[0]))
        if count_st_last == 0:
            count_csv = 0
        else:
            for i in range(count_csv):
                next(reader)

        for row in reader:
            if row['engTestStatus'] == 'Зараховано':
                values2 = (row['OUTID'], row['engPTRegName'], row['engTest'], row['engTestStatus'], float(row['engBall100'].replace(",", ".")), 2020, count_records+1, count_csv+1)
                cur.execute(query_ins, values2)
                count_records += 1
                conn.commit()
            count_csv += 1

    cur.execute('create view Help as select eng_region, avg(engball) as avg_19  from english19_20  where year = 2019 group by eng_region')
    cur.execute('create view Help2 as select eng_region, avg(engball) as avg_20 from english19_20  where year = 2020 group by eng_region')
    cur.execute('select *  from Help join Help2 using(eng_region)')

    with open('ZNOeng19_20_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['region', 'avgBall2019', 'avgBall2020', 'higherBallInYear']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        better = '2019'
        for row in cur:
            if row[2] > row[1]:
                better = 2020
            else:
                better = 2019
            writer.writerow({'region': row[0], 'avgBall2019': row[1], 'avgBall2020': row[2], 'higherBallInYear': better})

    cur.execute('drop view Help')
    cur.execute('drop view Help2')

    conn.commit()