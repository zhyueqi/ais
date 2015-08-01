from __future__ import print_function
from os import getenv
import pymssql
import re

server = "192.168.1.100"
user = "zrais"
password = "zr2015@whut"

conn = pymssql.connect(server, user, password, "AIS")


def query_all_tables():
    sql = 'SELECT * FROM information_schema.tables'
    cursor = conn.cursor()
    cursor.execute(sql)
    result = []
    for row in cursor:
        print('row = %r' % (row,))
        result.append(row[2])
    return result


def pos_table_filter(tbs):
    return [a for a in tbs if a.startswith('POS_INFO_TB_')]


dates = pos_table_filter(query_all_tables())

DATES = {}


pattern = re.compile('^POS_INFO_TB_(\d+)_(\d+)_(\d+)$')


def get_all_date(tbs):
    for i in tbs:
        match = pattern.match(i)
        year = match.group(1)
        month = match.group(2)
        day = match.group(3)
        if year not in DATES:
            DATES[year] = {}
        else:
            if month not in DATES[year]:
                DATES[year][month] = set()
            else:
                DATES[year][month].add(day)

    print(DATES)

get_all_date(dates)


def query_sql(sql):
    cursor = conn.cursor(as_dict=True)
    cursor.execute(sql)
    result = []
    for row in cursor:
        result.append(row)
    return result


def query_all_distict_MMSI(tb_name, process):
    sql = 'SELECT distinct(MMSI) as ship from ' + tb_name
    sql_count = 'SELECT COUNT(t.ship) as count from (' + sql + ') as t'
    result = query_sql(sql_count)
    count = result[0]['count']
    maxRow = 100
    # start = 0
    query_all_distict_MMSI.start = 0
    # end = maxRow
    query_all_distict_MMSI.end = maxRow

    def next_page():
        sql_page = 'SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (ORDER BY MMSI) as row FROM ' + \
            tb_name + ' ) as a WHERE a.row > ' + \
            str(query_all_distict_MMSI.start) + \
            ' and a.row <= ' + str(query_all_distict_MMSI.end)

        result = query_sql(sql_page)
        query_all_distict_MMSI.start += maxRow
        query_all_distict_MMSI.end += maxRow
        process(result)
        return next_page

    if count > maxRow:
        return next_page()
    else:
        return None


def query_count_rows(tb_name):
    sql = 'SELECT COUNT(MMSI) AS count from ' + tb_name
    cursor = conn.cursor(as_dict=True)
    cursor.execute(sql)
    count = 0
    for row in cursor:
        count = row['count']
    return count


def query_path(tb_name, mmsi, process):
    sql_count = 'SELECT COUNT(MMSI) as count from ' + \
        tb_name + ' WHERE MMSI=' + mmsi
    result = query_sql(sql_count)
    count = result[0]['count']
    maxRow = 100
    # start = 0
    query_path.start = 0
    # end = maxRow
    query_path.end = maxRow

    def next_page():
        sql_page = 'SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (ORDER BY REC_TIME) as row FROM ' + \
            tb_name + ' WHERE MMSI=' + mmsi + ') as a WHERE a.row > ' + \
            str(query_path.start) + \
            ' and a.row <= ' + str(query_path.end)

        result = query_sql(sql_page)
        query_path.start += maxRow
        query_path.end += maxRow
        process(result)
        return next_page

    if count > maxRow:
        return next_page()
    else:
        return None

query_count_rows(dates[0])
# query_all_tables()


def each(arr):
    for i in arr:
        print(i, '\n')
# page = query_all_distict_MMSI(dates[0], each)

page = query_path(dates[0], '200000528', each)

conn.close()
