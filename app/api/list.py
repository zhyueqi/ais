# coding:utf-8
from flask import request, jsonify
from . import api
from ..service import db


@api.route('/flask/api/list/tables', methods=['GET'])
def tables():
    ms = db.mssql()
    result = ms.query_all_tables()
    result = db.pos_table_filter(result)
    return jsonify({'code':1,'data': result})


@api.route('/flask/api/list/mmsi', methods=['GET'])
def query_all_distict_MMSI():
    tbName = request.args['table']
    ms = db.mssql()
    tables = ms.query_all_tables()
    tables = db.pos_table_filter(tables)
    if tbName not in tables:
        return jsonify({'code': 0, 'msg': 'table name does not exists!'})

    result = {'result': []}

    def getRows(row):
        result['result'] = row
    try:
        start = request.args['start']
        start = int(start)
        query = ms.query_all_distict_MMSI(tbName, getRows, start, 100)
        count = ms.query_count_all_distict_MMSI(tbName)
        return jsonify({'code': 1, 'data': result['result'],'count':count})

    except Exception as e:
        return jsonify({'code': 0, 'msg': 'server error ' + str(e)})


@api.route('/flask/api/list/path', methods=['GET'])
def query_path():
    tbName = request.args['table']
    mmsi = request.args['mmsi']
    ms = db.mssql()
    tables = ms.query_all_tables()
    tables = db.pos_table_filter(tables)
    if tbName not in tables:
        return jsonify({'code': 0, 'msg': 'table name does not exists!'})

    if mmsi == '' or mmsi == None:
        return jsonify({'code': 0, 'msg': 'mmsi  does not exists!'})

    result = {'result': []}

    def getRows(row):
        if row is None:
            result['result'] = []
            print 'none'
        else:
            result['result'] = row

    try:
        start = request.args['start']
        start = int(start)
        query = ms.query_path(tbName, mmsi, getRows, start, 1000)
        return jsonify({'code': 1, 'data': result['result']})

    except:
        return jsonify({'code': 0, 'msg': 'server error'})
