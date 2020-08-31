from mysql.connector import connect
from mysql.connector.errors import ProgrammingError
from contextlib import contextmanager
from decimal import Decimal
import csv

parms = dict(
    host='',
    port=0,
    user='',
    passwd='',
    database=''
)

@contextmanager
def instance():
    connection = connect(**parms)
    try:
        yield connection
    finally:
        if(connection and connection.is_connected()):
            connection.close()

def todecimal(string):
    if (string.endswith('%')):
        string = string.replace('.', '')
        string = string.replace(',', '.')
        string = string[:-1]
        return Decimal(string) / 100
    else:
        return string

def build_sql(keys):
    sql = 'INSERT INTO stocks('

    for key in keys:
        sql += key + ','

    sql += ') VALUES ('

    for index in range(0, len(keys)):
        sql += '%s,'

    sql += ')'
    sql = sql.replace(',)', ')')

    return sql

def add_papers(all_papers):
    aux = 0

    for paper in all_papers:
        
        sql = build_sql(paper.keys())
        paper_values = paper.values()
        paper_values = [todecimal(str(s)) for s in paper_values]

        with instance() as connection:
            try:
                cursor = connection.cursor()
                cursor.execute(sql, paper_values)
                connection.commit()
            except ProgrammingError as e:
                print(e.msg)
                pass
            else:
                aux += 1
                
    print('Foram adicionados ' + str(aux) + ' cadastros')

def export_csv(sufix):
    with instance() as connection:
        try:
            sql = 'SELECT * FROM safeinvest.stocks'
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            fp = open('./csv/'+sufix+'_stocks.csv', 'wb')
            myFile = csv.writer(fp)
            myFile.writerows(rows)
            fp.close()
        except ProgrammingError as e:
            print(e.msg)
            pass
        else:
            print('Exported')
