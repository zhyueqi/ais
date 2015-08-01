from __future__ import print_function
from os import getenv
import pymssql
import re

server = "192.168.1.100"
user = "zrais"
password = "zr2015@whut"

# conn = pymssql.connect(server, user, password, "AIS")

pattern = re.compile('^POS_INFO_TB_(\d+)_(\d+)_(\d+)$')


def pos_table_filter(tbs):
    return [a for a in tbs if a.startswith('POS_INFO_TB_')]


class mssql:
    conn = pymssql.connect(server, user, password, "AIS")

    def __del__(self):
        # self.conn.close()
        pass

    def query_all_tables(self):
        sql = 'SELECT * FROM information_schema.tables'
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = []
        for row in cursor:
            # print('row = %r' % (row,))
            result.append(row[2])
        return result

    def query_sql(self, sql):
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(sql)
        result = []
        for row in cursor:
            result.append(row)
        return result

    def query_all_distict_MMSI(self, tb_name, process, start=0, length=100):
        sql = 'SELECT distinct(MMSI) as ship from ' + tb_name
        sql_count = 'SELECT COUNT(t.ship) as count from (' + sql + ') as t'
        result = self.query_sql(sql_count)
        count = result[0]['count']
        maxRow = length
        # start = 0
        var = {}
        var['start'] = start
        # end = maxRow
        var['end'] = maxRow + start

        def next_page():
            sql_page = 'SELECT * FROM ( SELECT MMSI, ROW_NUMBER() OVER (ORDER BY MMSI) as row FROM ' + \
                tb_name + ' GROUP BY MMSI ) as a WHERE a.row > ' + \
                str(var['start']) + \
                ' and a.row <= ' + str(var['end'])

            result = self.query_sql(sql_page)
            var['start'] += maxRow
            var['end'] += maxRow
            process(result)
            return next_page

        if count > maxRow:
            return next_page()
        elif count > 0:
            next_page()
            return None
        else:
            return None

    def query_all_MMSI(self, tb_name, process, start_time, end_time, length=100):
        day = tb_name.split('TB_')[1].replace('_', '-')
        sql = "SELECT MMSI,REC_TIME FROM {} WHERE REC_TIME BETWEEN CONVERT(datetime, '{} {}') AND CONVERT(datetime, '{} {}')".format(
            tb_name, day, start_time, day, end_time)
        sql_count = 'SELECT COUNT(t.MMSI) as count FROM ({}) AS t'.format(sql)
        # print(sql_count)
        result = self.query_sql(sql_count)
        count = result[0]['count']
        maxRow = length
        var = {}
        var['start'] = 0
        # end = maxRow
        var['end'] = maxRow + 0

        def next_page():
            sql_page = "SELECT * FROM (SELECT [MMSI],ROW_NUMBER()OVER(ORDER BY[REC_TIME]) as row,[LON],[LAT],[COG],[SOG] FROM {} WHERE REC_TIME BETWEEN CONVERT(datetime,'{} {}') AND CONVERT(datetime, '{} {}')) as t WHERE t.row >0 AND t.row<={}".format(
                tb_name, day, start_time, day, end_time, maxRow)
            result = self.query_sql(sql_page)
            var['start'] += maxRow
            var['end'] += maxRow
            process(result)
            return next_page

        if var['end'] < count:
            return next_page()
        elif var['end'] >= count:
            next_page()
            return None
        else:
            return None

    def query_count_rows(self, tb_name):
        sql = 'SELECT COUNT(MMSI) AS count from ' + tb_name
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(sql)
        count = 0
        for row in cursor:
            count = row['count']
        return count

    def query_count_all_distict_MMSI(self, tbName):
        sql = 'SELECT COUNT(t.MMSI) AS count from ( SELECT distinct(MMSI) as MMSI from ' + \
            tbName + ' ) as t'
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(sql)
        count = 0
        for row in cursor:
            count = row['count']
        return count

    def query_path(self, tb_name, mmsi, process, start=0, length=100):
        sql_count = 'SELECT COUNT(MMSI) as count from ' + \
            tb_name + ' WHERE MMSI=' + mmsi
        result = self.query_sql(sql_count)
        count = result[0]['count']
        maxRow = length
        # start = 0
        var = {}
        var['start'] = start
        # end = maxRow
        var['end'] = start + maxRow

        def next_page():
            sql_page = 'SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (ORDER BY REC_TIME) as row FROM ' + \
                tb_name + ' WHERE MMSI=' + mmsi + ') as a WHERE a.row > ' + \
                str(var['start']) + \
                ' and a.row <= ' + str(var['end'])
            result = self.query_sql(sql_page)
            var['start'] += maxRow
            var['end'] += maxRow
            process(result)
            return next_page

        if count > maxRow:
            return next_page()
        elif count > 0:
            next_page()
            return None
        else:
            return None


if __name__ == '__main__':
    ms = mssql()

    def process(a):
        for i in a:
            print(a)

    ms.query_all_MMSI(
        'POS_INFO_TB_2014_07_04', process, '00:00:01', '00:03:00')
# conn.close()
