""" Storage DB Analyzer

    without debugging
        python csv_to_sqlite.py --asset-csv ASSET_20230524.csv --storage-csv STORAGE_20230524.csv
        
    with debugging
        python -m pdb csv_to_sqlite.py --asset-csv ASSET_20230524.csv --storage-csv STORAGE_20230524.csv
    
Returns:
    _type_: _description_
"""
import imp
import sqlite3
from venv import create
from dataclasses import dataclass
import logging
import csv
import argparse
import sys
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('csv_to_sqlite')

@dataclass
class TableInfo:
    table_name: str
    header: list
    data_filename: str
    count: int

def create_table(cur, ti:TableInfo):
    cols = [ f'"{a}"' for a in ti.header ]
    sql = f'CREATE TABLE {ti.table_name}({", ".join(cols)})'
    res = cur.execute(sql)
    return res

def create_database(cur, ti):
    try:
        return create_table(cur, ti)
    except sqlite3.OperationalError as e:
        logger.warning(e)
        return cur

def read_data(cur, fn):
    with open(fn, encoding='utf-8') as fd:
        reader = csv.reader(fd, delimiter=',')
        is_first = True
        data = []
        header = None
        for row in reader:
            if is_first:
                is_first = False
                header = row
                # remove UTF signature
                header[0] = row[0][1:]
                continue
            data.append(row)
            # logger.info(f'READ: {", ".join(row)}')
            # cur.execute('INSERT INTO Asset VALUES ' + fields,   )
        return header, data
    

def import_data(cur, ti:TableInfo, data):
    n = len(data[0])
    sql = f'INSERT INTO {ti.table_name} VALUES({", ".join(n*["?"])})'
    ret = cur.executemany(sql, data)
    return ret


def delete_database(database_name):
    p = Path(database_name)
    if p.exists():
        r = input(f'Database {p} already exists. Delete? (y/n)')
        if r.lower() == 'n':
            logger.info('Bye')
            return False
        elif r.lower() == 'y':
            p.unlink()
            return True
        else:
            logger.error('Unknown answer. Exiting.')
            return False
    return True

def count_records(cur, table_name):
    ret = cur.execute(f'select count(*) from {table_name}')
    return ret.fetchone()[0]

def main():
    parser = argparse.ArgumentParser(description='Import Asset and Storage data into database')
    parser.add_argument('--asset-csv', type=str, help='Asset csv file', required=True)
    parser.add_argument('--storage-csv', type=str, help='Storage csv file', required=True)
    parser.add_argument('--database-name', type=str, help='Database file', default='data.db')
    args = parser.parse_args()
    data_list = {
        # 'Asset': 'ASSET_20230524.csv',
        # 'Storage': 'STORAGE_20230524.csv',
        'Asset': args.asset_csv,
        'Storage': args.storage_csv,
    }

    if not delete_database(args.database_name):
        return
    
    con = sqlite3.connect(args.database_name)
    cur = con.cursor()

    for table_name, fn in data_list.items():
        header, data = read_data(cur, fn)
        ti = TableInfo(table_name, header, fn, 0)
        create_database(cur, ti)
        ret = import_data(cur, ti, data)
        n_imported = ret.rowcount
        n_total = count_records(cur, ti.table_name)
        logger.info(f'{ti.table_name} : Imported {n_imported} records, Total {n_total} records.')
        con.commit()
    con.close()


if __name__ == '__main__':
    main()
